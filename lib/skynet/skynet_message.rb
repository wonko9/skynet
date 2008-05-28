class Skynet
  
  class Message

    include SkynetDebugger
    
    class BadMessage < Skynet::Error; end
    
    class << self
      attr_accessor :fields
    end
    
    attr_reader :tasktype
          

    def self.set_fields(fields)
      self.fields = fields
      self.fields.each do |method| 
        next if [:task, :tasktype].include?(method)
        attr_accessor method
      end
    end

    def initialize(opts)
      if opts.is_a?(Array)
        self.class.fields.each_with_index do |field, ii|
          self.send("#{field}=",opts[ii] || nil)
        end
      elsif opts
        self.class.fields.each do |field|
          value = opts[field] || opts[field.to_s] || nil
          self.send("#{field}=",value) if value
        end
      end
      self.task
    end
                     
    def fields
      self.class.fields
    end
    
    def tasktype=(ttype)
      if ttype.nil?
        @tasktype=nil
      else
        @tasktype = ttype.to_sym
      end
    end
        
    def task=(data)  
      @task = data
      self.raw_task = data.to_yaml if data.respond_to?(:to_yaml) and not task.kind_of?(Proc)
    end
        
    def task    
      @task ||= begin
          YAML::load(self.raw_task) if self.raw_task
        rescue Exception => e
          raise BadMessage.new("Couldnt marshal task #{e.inspect} #{e.backtrace.join("\n")}")
        end
    end

    def raw_task=(data)
      @raw_task = data
      @task=nil
    end

    def raw_task
      @raw_task
    end
        def [](ii)
      send(self.class.fields[ii])
    end
    
    def to_a
      self.class.fields.collect do |field|
        self.send(field)
      end
    end
    
    def to_hash
      hash = {}
      self.class.fields.each do |field|
        hash[field] = self.send(field)
      end                                          
      hash
    end   
    
    def to_h
      to_hash
    end
  
    def to_s
      to_a
    end
  
  
    ## FIXME REMOVE
    def self.outstanding_tasks_template(iteration=nil,queue_id=0)
      template = {
        :tasktype  => :task,
        :queue_id  => queue_id,
        :iteration => iteration
      }
      fields.collect do |field|
        template[field]
      end
    end  

    ## FIXME REMOVE
    def self.outstanding_results_template(queue_id=0)
      template = {
        :tasktype => :result,
        :queue_id => queue_id
      }
      fields.collect do |field|
        template[field]
      end
    end
      
  end  ## END class Message


  class ResultMessage < Message

    self.set_fields([
     :task_id,
     :job_id,
     :task, ### FIXME Should results have the orig task?
     :result,
     :name,
     :result_timeout,
     :expire_time,
     :iteration,
     :version,
     :retry,
     :queue_id,
     :result_code,
     :sn_result_code,
    ])
  
    def tasktype
      :result
    end

    def timeout
      result_timeout
    end
    
    def timeout=(timeout)
      result_timeout
    end
    
    def payload
      result
    end
    
  end

  class ErrorMessage < ResultMessage

    self.set_fields([
     :task_id,
     :job_id,
     :task, ### FIXME Should results have the orig task?
     :error,
     :name,
     :result_timeout,
     :expire_time,
     :iteration,
     :version,
     :retry,
     :queue_id,
     :result_code,
     :sn_result_code,
    ])
    
  
    def tasktype
      :error
    end
    
    def payload
      error
    end
    
    def error=(error)
      self.result=error
    end
    
    def error
      result
    end
    
  end

  
  class TaskMessage < Message

    self.set_fields([
     :tasktype, ## replaces payload_type.   :master, :task, :result, :error
     :task_id,
     :job_id,
     :task,
     :error,
     :name,
     :start_after,
     :timeout,
     :result_timeout,
     :expire_time,
     :iteration,
     :version,
     :retry,
     :queue_id
    ])
    

    def initialize(opts)
      if opts.is_a?(Array)
        self.class.fields.each_with_index do |field, ii|
          self.send("#{field}=",opts[ii] || nil)
        end
      elsif opts
        self.class.fields.each do |field|
          value = opts[field] || opts[field.to_s] || nil
          self.send("#{field}=",value) if value
        end
        opts_raw_task = opts[:raw_task] || opts["raw_task"]
        if opts_raw_task
          self.raw_task = opts_raw_task
        end
        self.retry ||= 0
      end
      self.task
    end

    def payload
      task
    end

    def retry?
      return false unless self.retry
      
      if self.retry
        if (self.retry and self.iteration >= self.retry)
          false
        else
          true
        end
      # FIXME The default retries should be SET in the message object by Skynet::job!!!
      # Originally I was gonna do this for map and reduce, but we don't know that here, just whether its a master.
      elsif self.tasktype.to_sym == :master and Skynet::CONFIG[:DEFAULT_MASTER_RETRY] and self.iteration >= Skynet::CONFIG[:DEFAULT_MASTER_RETRY]
        false
      elsif Skynet::CONFIG[:MAX_RETRIES] and self.iteration >= Skynet::CONFIG[:MAX_RETRIES]
        false
      else
        false
      end
    end
        
    def run_task
      raise BadMessage.new("run_task expecting type Skynet::Task got #{task.class}") unless task.is_a?(Skynet::Task)
      raise Error.new("This task has reached max retries. Current itteration: #{iteration}, Retries: #{self.retry}")
      info "running task #{name} TIMEOUT: #{timeout} task_id:#{task_id} MorR:#{task.map_or_reduce} PROCESS CLASS: #{task.process.class}"
      begin
        return result_message(task.run(iteration))

      ## If we have an exception, how do we know whether we should acknowledge the first message or not
      rescue Skynet::Task::Error => e
        error "TASK ERROR: #{e.orig_exception.message} #{e.orig_exception.backtrace.join("\n")}"
        ### Need to handle these errors better
        return error_message(task, e)
      rescue Exception => e
        return error_message(task, e)
      end      
    end

    def self.new_task_message(task,job)
      self.new(
        :tasktype       => task.task_or_master,
        :task_id        => task.task_id,
        :job_id         => job.job_id,
        :task           => task,
        :name           => task.name,       
        :start_after    => job.start_after,
        :timeout        => task.timeout,
        ## FIXME Where would we get this from the task? Should we be making these from tasks?
        :result_timeout => task.result_timeout,
        # :expire_time  => Time.now.to_i + task.result_timeout ### expire time doesn't have to be stored
        :version        => job.version,
        :queue_id       => job.queue_id || 0,
        :iteration      => 0,
        :retry          => task.retry
      )
    end

    ## FIXME Do we need a next task template?
    # def self.next_task_template(version=nil, tasktype=nil, queue_id=0)
    #   template = {                           
    #     ### FIXME expire_time should just be a number
    #     :expire_time  => (0 .. Time.now.to_i),
    #     :tasktype     => :task,
    #     :queue_id     => queue_id,
    #     :version      => version,
    #     :iteration    => (0..Skynet::CONFIG[:MAX_RETRIES]),
    #   }
    # 
    #   fields.collect do |field|
    #     template[field]
    #   end
    # end
    
    def self.result_message(message,result)
      template = {                        
        :result         => result,
        :result_timeout => message.result_timeout,
      }

      Skynet::ResultMessage.new(message.to_h.merge(template))
    end

    def self.fallback_task_message(message)
       template = {}
       if message.retry?
         template[:iteration] = message.iteration + 1
         template[:expire_time] = Time.now.to_i + message.timeout
       else
         return nil
       end

       fields.each do |field|
         template[field] = message.send(field) unless template.has_key?(field)
       end
       # debug "BUILDING NEXT FALLBACK TASK MESSAGE OFF"#, template
       Skynet::TaskMessage.new(template)
    end      
  
    def fallback_task_message
      self.class.fallback_task_message(self)
    end

    def result_message(result)
      self.class.result_message(self,result)
    end

    def self.error_message(message,error)
      template = {
        :error          => error,
        :result_timeout => message.result_timeout,
      }

      fields.each do |field|
        template[field] = message.send(field) unless template.has_key?(field)
      end
      Skynet::ErrorMessage.new(message.to_h.merge(template))
    end
  
    def error_message(error)
      self.class.error_message(self,error)
    end


  end

  
  
  # class WorkerVersionMessage < Skynet::Message
  #   
  #   self.fields = [:version,:expire_time]
  # 
  #   def initialize(opts)
  #     super
  #     self.expire_time ||= Time.now.to_i
  #     self.tasktype    = :current_worker_rev
  #   end
  #   
  #   def version
  #     @version.to_i
  #   end
  # 
  #   def self.template
  #     template = {
  #       :tasktype  => :current_worker_rev
  #     }
  #     fields.collect do |field|
  #       template[field]
  #     end
  #   end
  #   
  #   def template
  #     template = {
  #       :tasktype    => :current_worker_rev,
  #       :expire_time => nil
  #     }
  #     fields.collect do |field|
  #       template[field] || self.send(field)
  #     end
  #   end
  # 
  # end


  class WorkerStatusMessage < Skynet::Message
# I'd love to rename tasktype and tasksubtype to something more reasonable
    self.fields = [
      :tasktype,
      :tasksubtype,
      :worker_id,
      :hostname,
      :process_id,
      :job_id,
      :task_id,
      :iteration,
      :name,
      :map_or_reduce,
      :started_at,
      :version,
      :processed,
      :queue_id
    ]
    self.fields.each { |method| attr_accessor method }

    def initialize(opts)
      super
      self.tasksubtype = :worker
    end
    
    ### REMOVE TEMPLATES
    def self.worker_status_template(opts)
      template = {
        :tasksubtype => :worker,
        :hostname    => opts[:hostname],
        :process_id  => opts[:process_id]        
      }
      fields.collect do |field|
        template[field]
      end
    end
    
    def self.all_workers_template(hostname=nil)
      template = {
        :tasksubtype => :worker,
        :hostname    => hostname,
      }
      fields.collect do |field|
        template[field]
      end
    end    
  end # class WorkerStatusMessage

end
