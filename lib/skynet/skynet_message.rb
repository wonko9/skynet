class Skynet
  class Message

    include SkynetDebugger
    
    class BadMessage < Skynet::Error
    end
    
    class << self
      attr_accessor :fields
    end

    self.fields = {
      0  => :tasktype,
      1  => :drburi,
      2  => :task_id,
      3  => :job_id,
      4  => :payload,
      5  => :payload_type,
      6  => :name,
      7  => :expiry,
      8  => :expire_time,
      9  => :iteration,
      10 => :version,
      11 => :retry
    }

    self.fields.values.each do |method| 
      next if method == :payload
      next if method == :tasktype
      next if method == :payload_type
      attr_accessor method
    end
    
    attr_reader :payload_type, :tasktype
  
    def initialize(opts)
      if opts.is_a?(Array)
        self.class.fields.each do |ii, field|
          self.send("#{field}=",opts[ii] || nil)
        end
      elsif opts
        self.class.fields.values.each do |field|
          value = opts[field] || opts[field.to_s] || nil
          self.send("#{field}=",value) if value
        end
        opts_raw_payload = opts[:raw_payload] || opts["raw_payload"]
        if opts_raw_payload
          self.raw_payload = opts_raw_payload
        end
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
      self.class.fields.keys.sort.collect do |ii|
        self.send(self.class.fields[ii])
      end
    end
    
    def to_hash
      hash = {}
      self.class.fields.keys.sort.collect do |ii|
        hash[self.class.fields[ii]] = self.send(self.class.fields[ii])
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

    ####### TEMPLATES ############
    
    def self.next_task_template(version=nil,payload_type=nil)
      fields.keys.sort.collect do |ii|
        field = fields[ii]
        case field
        when :expire_time
          (0 .. Time.now.to_i)
        when :tasktype
          :task
        when :version
          version
        when :payload_type
          payload_type
        when :iteration
          (0..Skynet::CONFIG[:MAX_RETRIES])
        else
          nil
        end
      end
    end
  
    def self.result_template(job_id,tasktype=:result)
      fields.keys.sort.collect do |ii|
        field = fields[ii]
        case field
        when :tasktype
          tasktype
        when :job_id
          job_id
        else
          nil
        end
      end      
    end
  
    def self.result_message(message,result,tasktype=:result, resulttype=:result)
      message_array = fields.keys.sort.collect do |ii|
        field = fields[ii]
        case field      
        when :tasktype
          tasktype
        when :payload
          result
        when :payload_type
          resulttype
        else
          message.send(fields[ii])
        end
      end
      new(message_array)
    end
  
    def result_message(result,tasktype=:result, resulttype=:result)
      self.class.result_message(self,result,tasktype,resulttype)
    end
  
    def self.outstanding_tasks_template(iteration=nil)
      fields.keys.sort.collect do |ii|
        field = fields[ii]
        case field
        when :tasktype
          :task
        when :iteration
          iteration
        else
          nil
        end
      end
    end  

    def self.outstanding_results_template
      fields.keys.sort.collect do |ii|
        field = fields[ii]
        case field
        when :tasktype
          :result
        else
          nil
        end
      end
    end
  
    def self.error_message(message,error)
      result_message(message,error,:result,:error)
    end
  
    def error_message(error)
      self.class.error_message(self,error)
    end
  
    def self.error_template(message)
      fields.keys.sort.collect do |ii|
        field = fields[ii]
        case field
        when :tasktype
          message.tasktype
        when :drburi, :version, :task_id
          message.send(fields[ii])
        else
          nil
        end
      end
    end
    
    def error_template
      self.class.error_template(self)
    end  

    def self.fallback_task_message(message)
       opts = Hash.new
       fields.values.each do |field|
         case field
         when :iteration
           if (message.retry and message.iteration >= message.retry)
             opts[:iteration] = -1

           # Originally I was gonna do this for map and reduce, but we don't know that here, just whether its a master.
           elsif message.payload_type.to_sym == :master and Skynet::CONFIG[:DEFAULT_MASTER_RETRY] and message.iteration >= Skynet::CONFIG[:DEFAULT_MASTER_RETRY]
             opts[:iteration] = -1           
           elsif Skynet::CONFIG[:MAX_RETRIES] and message.iteration >= Skynet::CONFIG[:MAX_RETRIES]
             opts[:iteration] = -1           
           else
             opts[:iteration] = message.iteration + 1
           end
         when :expire_time
           opts[:expire_time] = Time.now.to_i + message.expiry
         else
           opts[field] = message.send(field)
         end
       end
       # debug "BUILDING NEXT FALLBACK TASK MESSAGE OFF"#, opts
       Skynet::Message.new(opts)
    end      
  
    def fallback_task_message
      self.class.fallback_task_message(self)
    end

    def self.fallback_template(message)
      fields.keys.sort.collect do |ii|
        field = fields[ii]
        case field
        when :tasktype
          message.tasktype
        when :drburi, :version, :task_id
          message.send(field)
        when :iteration
          (1..Skynet::CONFIG[:MAX_RETRIES])
        else
          nil
        end
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
      fields.keys.sort.collect do |ii|
        field = fields[ii]
        case field
        when :tasktype
          :current_worker_rev
        else
          nil
        end
      end
    end
    
    def template
      fields.keys.sort.collect do |ii|
        field = fields[ii]
        case field
        when :tasktype
          :current_worker_rev
        when :expire_time
          nil
        else
          self.send(field)        
        end
      end
    end
  end

  class WorkerStatusMessage < Skynet::Message
    self.fields = {
      0  => :tasktype,
      1  => :tasksubtype,
      2  => :worker_id,
      3  => :hostname,
      4  => :process_id,
      5  => :job_id,
      6  => :task_id,
      7  => :iteration,
      8  => :name,
      9  => :map_or_reduce,
      10 => :started_at,
      11 => :version,
      12 => :processed
    }
    self.fields.values.each { |method| attr_accessor method }

    def initialize(opts)
      super
      self.tasktype    = :status
      self.tasksubtype = :worker
    end
    
    def self.worker_status_template(opts)
      fields.keys.sort.collect do |key|      
        case fields[key]
        when :tasktype : :status
        when :tasksubtype : :worker
        when :hostname : opts[:hostname]
        when :process_id : opts[:process_id]
        else 
          nil
        end
      end
    end
    
    def self.all_workers_template(hostname=nil)
      fields.keys.sort.collect do |key|      
        case fields[key]
        when :tasktype : :status
        when :tasksubtype : :worker
        when :hostname 
          hostname if hostname
        else 
          nil
        end
      end
    end
    
  end

end
