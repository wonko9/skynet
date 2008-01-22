class Skynet
  class Message

    include SkynetDebugger
    
    class BadMessage < Skynet::Error; end
    
    class << self
      attr_accessor :fields
    end

    self.fields = [
     :tasktype,
     :drburi,
     :task_id,
     :job_id,
     :payload,
     :payload_type,
     :name,
     :expiry,
     :expire_time,
     :iteration,
     :version,
     :retry,
     :queue_id
    ]

    self.fields.each do |method| 
      next if [:payload, :tasktype, :payload_type].include?(method)
      attr_accessor method
    end
    
    attr_reader :payload_type, :tasktype
    
    def self.new_task_message(task,job)
      self.new(
        :job_id       => job.job_id,
        :expire_time  => job.start_after,
        :version      => job.version,
        :queue_id     => job.queue_id || 0,
        :iteration    => 0,
        :tasktype     => :task, 
        :task_id      => task.task_id,
        :payload      => task,
        :payload_type => task.task_or_master,
        :expiry       => task.result_timeout, 
        :name         => task.name,       
        :retry        => task.retry
      )
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
        opts_raw_payload = opts[:raw_payload] || opts["raw_payload"]
        if opts_raw_payload
          self.raw_payload = opts_raw_payload
        end
        self.retry ||= 0
      end
      self.payload      
    end
                     
    def fields
      self.class.fields
    end
    
    def tasktype=(ttype)
      @tasktype = ttype.to_sym
    end

    def payload_type=(ptype)
      @payload_type = ptype.to_sym if ptype
    end
    
    # alias for payload
    def task
      payload
    end
    
    def payload=(data)  
      @payload = data
      self.raw_payload = data.to_yaml if data.respond_to?(:to_yaml) and not payload.kind_of?(Proc)
    end
        
    def payload    
      @payload ||= begin
          YAML::load(self.raw_payload) if self.raw_payload
        rescue Exception => e
          raise BadMessage.new("Couldnt marshal payload #{e.inspect} #{e.backtrace.join("\n")}")
        end
    end

    def raw_payload=(data)
      @raw_payload = data
      @payload=nil
    end

    def raw_payload
      @raw_payload
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

    def timeout
      expire_time * 2
    end

    def self.next_task_template(version=nil, payload_type=nil, queue_id=0)
      template = {
        :expire_time  => (0 .. Time.now.to_i),
        :tasktype     => :task,
        :queue_id     => queue_id,
        :version      => version,
        :payload_type => payload_type,
        :iteration    => (0..Skynet::CONFIG[:MAX_RETRIES]),
      }

      fields.collect do |field|
        template[field]
      end
    end
  
    def self.result_template(job_id,tasktype=:result)
      template = {
        :tasktype => tasktype,
        :job_id   => job_id
      }
      fields.collect do |field|
        template[field]
      end
    end
  
    def self.result_message(message,result,tasktype=:result, resulttype=:result)
      template = {
        :tasktype     => tasktype,
        :payload      => result,
        :payload_type => resulttype
      }

      fields.each do |field|
        template[field] ||= message.send(field)
      end
      new(template)
    end
  
    def result_message(result,tasktype=:result, resulttype=:result)
      self.class.result_message(self,result,tasktype,resulttype)
    end
  
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

    def self.outstanding_results_template(queue_id=0)
      template = {
        :tasktype => :result,
        :queue_id => queue_id
      }
      fields.collect do |field|
        template[field]
      end
    end
  
    def self.error_message(message,error)
      result_message(message,error,:result,:error)
    end
  
    def error_message(error)
      self.class.error_message(self,error)
    end
  
    def self.error_template(message)
      template = {
        :tasktype  => message.tasktype,
        :drburi    => message.drburi, 
        :version   => message.version, 
        :task_id   => message.task_id, 
        :queue_id  => message.queue_id
      }
      fields.collect do |field|
        template[field]
      end
    end
    
    def error_template
      self.class.error_template(self)
    end  

    def self.fallback_task_message(message)
       template = {}
       if message.retry
         if (message.retry and message.iteration >= message.retry)
           template[:iteration] = -1
         else
           template[:iteration] = message.iteration + 1
         end
       # Originally I was gonna do this for map and reduce, but we don't know that here, just whether its a master.
       elsif message.payload_type.to_sym == :master and Skynet::CONFIG[:DEFAULT_MASTER_RETRY] and message.iteration >= Skynet::CONFIG[:DEFAULT_MASTER_RETRY]
         template[:iteration] = -1           
       elsif Skynet::CONFIG[:MAX_RETRIES] and message.iteration >= Skynet::CONFIG[:MAX_RETRIES]
         template[:iteration] = -1           
       else
         template[:iteration] = message.iteration + 1
       end

       template[:expire_time] = Time.now.to_i + message.expiry

       fields.each do |field|
         template[field] ||= message.send(field)
       end
       # debug "BUILDING NEXT FALLBACK TASK MESSAGE OFF"#, template
       Skynet::Message.new(template)
    end      
  
    def fallback_task_message
      self.class.fallback_task_message(self)
    end

    def self.fallback_template(message)
      template = {
        :tasktype  => message.tasktype,
        :drburi    => message.drburi, 
        :version   => message.version, 
        :task_id   => message.task_id, 
        :queue_id  => message.queue_id,        
        :iteration => (1..Skynet::CONFIG[:MAX_RETRIES]),        
      }
      fields.collect do |field|
        template[field]
      end
    end
  
    def fallback_template
      self.class.fallback_template(self)
    end

  end  ## END class Message

  class WorkerVersionMessage < Skynet::Message
    
    self.fields = self.superclass.fields
  
    def initialize(opts)
      super
      self.expire_time ||= Time.now.to_i
      self.tasktype    = :current_worker_rev
    end
    
    def version
      @version.to_i
    end

    def self.template
      template = {
        :tasktype  => :current_worker_rev
      }
      fields.collect do |field|
        template[field]
      end
    end
    
    def template
      template = {
        :tasktype    => :current_worker_rev,
        :expire_time => nil
      }
      fields.collect do |field|
        template[field] || self.send(field)
      end
    end
  end

  class WorkerStatusMessage < Skynet::Message
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
      self.tasktype    = :status
      self.tasksubtype = :worker
    end
    
    def self.worker_status_template(opts)
      template = {
        :tasktype    => :status,
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
        :tasktype    => :status,
        :tasksubtype => :worker,
        :hostname    => hostname,
      }
      fields.collect do |field|
        template[field]
      end
    end    
  end # class WorkerStatusMessage

end
