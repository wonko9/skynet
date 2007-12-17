class Skynet
  class Task
    
    include SkynetDebugger
        
    require 'ostruct'

    class ConstructorError < StandardError
    end
    
    attr_reader :data, :process, :result, :map_or_reduce
    attr_accessor :name, :tuple, :result_timeout
    
    @@log = nil
    
    def initialize(opts = {})
      unless opts[:task_id] and opts[:process] and opts[:map_or_reduce]
        raise ConstructorError.new("Must provide task_id, process and map_or_reduce")      
      end
      @marshalable = true
      @task_id = opts[:task_id].to_i
      @data    = opts[:data]
      self.process = opts[:process]
      @name    = opts[:name]
      @map_or_reduce = opts[:map_or_reduce]
      @result_timeout = opts[:result_timeout]
    end
    
    def process=(process)
      if process.is_a?(Proc)
        @marshalable = false
      end
    end  
    
    def can_marshal?
      @marshalable
    end
    
    def task_or_master
      if @map_or_reduce == :master
        @map_or_reduce 
      else
        :task
      end
    end

    def task_id
      @task_id.to_i
    end
    
    def run
      debug "running task #{name} task_id:#{task_id} MorR:#{map_or_reduce} PROCESS CLASS: #{@process.class}"
      begin
        if @process.class == Proc
          debug " - #{@map_or_reduce} using Proc"
          @process.call @data
        elsif @map_or_reduce == :master
          debug " - as master"
          job = Skynet::Job.new(@process)
          job.run
        elsif @process.class == String
          debug " - #{@map_or_reduce} using class #{@process}"
          @process.constantize.send(@map_or_reduce,@data)
        end
      rescue Exception => e
        error "Error running task #{e.inspect} TASK:", self, e.backtrace.join("\n")
      end
    end
    
    def self.debug_class_desc
      "TASK"
    end
  end  ## END class Task
end