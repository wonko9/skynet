class Skynet

  class MessageQueueAdapter::SingleProcess < Skynet::MessageQueueAdapter
    ## THIS SHOULD NOT BE HERE
    # def list_results(data,timeout=nil)
    #   raise AbstractClassError.new("You must implement list_results in a subclass.")
    # end    

    @@tasks = Hash.new(Hash.new([]))
    @@results = {}
    @@queues_inited = false
    @@worker_version = 0
        

    
    def initialize(options={})
      @failover = {}
      # @thread = Thread.new do
      #   loop do
      #     begin
      #       @failover.each do |task_id,task|
      #         # Grab expired tasks and put them back on the Q
      #         next if task.expire_time.to_i < Time.now.to_i
      #         ## FIXME Expire time should never be a range!  We have to fix this in Skynet::Message
      #         next if task.expire_time.is_a?(Range) and task.expire_time.last < Time.now.to_i
      #         unacknowledge(task)
      #         write_failover(task)
      #       end
      #     rescue Exception => e
      #       pp "ERROR", e, e.backtrace.join("\n")
      #     end
      #     sleep 0.1
      #   end
      # end
      # init_queues_if_needed
    end    
    
    # This is only for this single process testing
    def reqeue_expired_unacknowlged_tasks
      @failover.each do |task_id,task|
        now = Time.now.to_f
        # Grab expired tasks and put them back on the Q
        next if task.expire_time.to_f > now
        ## FIXME Expire time should never be a range!  We have to fix this in Skynet::Message
        next if task.expire_time.is_a?(Range) and task.expire_time.last > now
        unacknowledge(task)
        write_failover(task)
      end
    end
    
    def flush
      @failover = {}
      @@tasks = Hash.new(Hash.new([]))
      @@results = {}
    end
      
    # def init_queues_if_needed
    #   if @@queues_inited
    #     [:task,:master] do |tasktype|
    #       Thread.new do
    #         loop do
    #           task = @@task_queue.get(tasktype)
    #           @@tasks[task.queue_id][task.tasktype] << task
    #         end
    #       end
    #     end
    # 
    #     Thread.new do
    #       loop do
    #         task = @@result_queue.get(tasktype)
    #         @@tasks[task.queue_id][task.tasktype] << task
    #       end
    #     end
    #   end
    # end
  
    ## Is this necessary?  How can a queue do this!!!
    # def list_tasks(template,timeout=nil)
    #   @@tasks.values.values.join if @@tasks.values.any? and tasks.values.values.any?
    # end
    
    #subclass
    def dequeue_task(version,tasktype,timeout=1,queue_id=0)
      reqeue_expired_unacknowlged_tasks
      tasktype = tasktype.to_s
      raise Error.new("#{tasktype} is not a valid payload type.  Only allowed types are #{VALID_PAYLOAD_TYPES.join(',')}") unless VALID_PAYLOAD_TYPES.include?(tasktype.to_s)
      start = Time.now
      loop do              
        # Handle workers that want to grab masters, tasks, or either
         real_tasktype = tasktype
         if tasktype == "master_or_task"
           real_tasktype = ["master","task"].rand
         end
         
        # pp "LOOKING IN @@tasks[#{queue_id}][#{tasktype}]", @@tasks[queue_id][tasktype]
        @@tasks[queue_id][tasktype].each_with_index do |task,ii|

          ## only take tasks that are ready to be worked on
          next if task.expire_time.to_i > Time.now.to_i
          ## FIXME Expire time should never be a range!  We have to fix this in Skynet::Message
          next if task.expire_time.is_a?(Range) and task.expire_time.last > Time.now.to_i
                  
          check_task_version(task)
        
          ## dequeue
          message = @@tasks[queue_id][tasktype].slice!(ii)
                    
          ## We're simulating how a worker might fail catostrophically without acknowledging or not
          # First we see if this message is eligible for retry
          # Maybe we can abstract this as accept_unacknowledged_method or something

          return message
        end        
        raise Skynet::RequestExpiredError.new if timeout and Time.now > start + timeout
      end
    end
    
    def accept_unacknowledged_message(message)
      if message.retry?
        fallbackmessage = message.clone
        ## The problem is, we have to start counting from now to the timeout, maybe Message should do this for us somehow?
        # FIXME Should this be here?  When does the clock start?
        fallbackmessage.expire_time = Time.now.to_i + fallbackmessage.timeout
        ## This particular adapter uses an internal queue of tasks to make sure the workers respond in time. If not, they reput
        ## them on the queue anyway.  Since this is single process, it's not likely there would be more than one task 
        ## on here at a time
        @failover[message.task_id] = fallbackmessage
      end
    end

    #subclass
    def acknowledge(message)
      @failover.delete(message.task_id) 
    end
    
    #subclass
    def unacknowledge(message)
      ## How do we unacknowledge
      @failover.delete(message.task_id) 
    end

    #subclass
    def enqueue_task(message)
      @@tasks[message.queue_id][message.tasktype].push(message)
    end

    # subclass this
    # do we need timeout?
    def enqueue_result(message)
      # First we'll make sure that task is acknowledged so it is taken off the Q permanently
      acknowledge(message)
      @@results[message.job_id] ||= []
      @@results[message.job_id] << message
    end
    
    #subclass
    def dequeue_result(job_id,timeout=1)
      ## Before a worker adds tasks to the task_queue, it creates a results queue for itself
      ## It subscribes to that queue to watch for results
      start = Time.now
      loop do
        if @@results[job_id]
          if @@results[job_id].empty?
            @@results.delete(job_id) 
            next
          else
            return @@results[job_id].pop
          end
        end        
        raise Skynet::RequestExpiredError.new if timeout and Time.now > start + timeout
      end
    end    
                
    #subclass
    def connected?
      true
    end   
    
    ## There should be central code for determining the current worker version.  
    ## Does the new version matter if we know we are of the wrong version?
    def set_worker_version(version)
      @@worker_version = version
    end
  
    ## Supposed to determine the global current worker version
    ## Does the new version matter if we know we are of the wrong version?
    def get_worker_version
      @@worker_version
    end
  
    ## Is this version still active in the queue?
    def version_active?(curver=nil, queue_id = 0)    
      true
    end
    
    ## flush task queue
    def flush_tasks
      @@tasks = Hash.new(Hash.new([]))      
    end  
  
    ## flush task queue
    ## Is this possible?
    def flush_results
      @@results = Hash.new([])
    end
  end
end
