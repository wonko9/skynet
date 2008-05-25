class Skynet

  class MessageQueueAdapter::QuiQ < Skynet::MessageQueueAdapter
    # def list_results(data,timeout=nil)
    #   raise AbstractClassError.new("You must implement list_results in a subclass.")
    # end

    @@tasks = Hash.new(Hash.new([]))
    @@results = Hash.new(Hash.new([]))
    @@queues_inited = false
    @@worker_version = 0
  
    # @@task_queue = SingleQueues.new(:tasks)
    # @@results_queue = SingleQueues.new(:results)
      
    def initialize(options={})
      # init_queues_if_needed
    end    
  
    # def init_queues_if_needed
    #   if @@queues_inited
    #     [:task,:master] do |payload_type|
    #       Thread.new do
    #         loop do
    #           task = @@task_queue.get(payload_type)
    #           @@tasks[task.queue_id][task.payload_type] << task
    #         end
    #       end
    #     end
    # 
    #     Thread.new do
    #       loop do
    #         task = @@result_queue.get(payload_type)
    #         @@tasks[task.queue_id][task.payload_type] << task
    #       end
    #     end
    #   end
    # end
  
    ## Is this necessary?  How can a queue do this!!!
    def list_tasks(template,timeout=nil)
      @@tasks.values.values.join if @@tasks.values.any? and tasks.values.values.any?
    end

    def take_next_task(curver,timeout=1,payload_type=nil,queue_id=0)
      start = Time.now
      loop do
        @@tasks[queue_id][payload_type].each_with_index do |task,ii|

          ## only take tasks that are ready to be worked on
          next unless task.expire_time.is_a?(Range) and task.expire_time.last < Time.now.to_i
        
          ## IF this task isn't the right version and we find there's a new version, raise
          raise Skynet::NewWorkerVersion.new if curver != task.version and curver != get_worker_version
          ## Make sure NOT to dequeue task if we raised above
        
          ## dequeue
          return @@tasks[queue_id][payload_type].slice!(ii)
        end        
        raise Skynet::RequestExpiredError.new if timeout and Time.now > start + timeout
      end
    end

    def write_task(task,timeout=nil)
      # There are only two payload types, :task and :master
      @@tasks[task.queue_id][task.payload_type] << task

      # @@task_queue.put(task,task.payload_type,"#{task.queue_id}")
    end
  
    def write_message(task,timeout=nil)
      write_task(task,timeout)
    end

    def write_result(task,timeout=nil)
      ## It is assumed a queue was created for the original publisher of the task.  
      ## Here we'll add this task to that publisher's queue
      @@results[task.queue_id][task.task_id]
    end

    def take_result(task,timeout=1)
      ## Before a worker adds tasks to the task_queue, it creates a results queue for itself
      ## It subscribes to that queue to watch for results
      start = Time.now
      loop do
        if @@results[queue_id][task.task_id]
          @@results[queue_id].delete(task.task_id)
        end        
        raise Skynet::RequestExpiredError.new if timeout and Time.now > start + timeout
      end
    end

    def write_error(message,timeout=nil)
      ## Errors are forms of results and go back on the results queue
      task = message.error_message(message)
      @@results[task.queue_id][task.task_id]
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
      @@results = Hash.new(Hash.new([]))
    end
  end
end
