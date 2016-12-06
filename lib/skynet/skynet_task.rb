class Skynet
  class Task
    include SkynetDebugger

    class ConstructorError < StandardError; end
    class TimeoutError     < StandardError; end

    attr_reader :data, :process, :result, :map_or_reduce, :marshalable
    attr_accessor :name, :result_timeout, :retry

    @@log = nil

    def self.debug_class_desc
      "TASK"
    end

    def self.master_task(job)
      options = {
        :async        => false,
        :local_master => true,
        :map_name     => job.map_name || job.name,
        :reduce_name  => job.reduce_name || job.name,
      }
      Skynet::Job::FIELDS.each do |field|
        next if options.has_key?(field)
        options[field] = job.send(field) if job.send(field)
      end

      master_job = Skynet::Job.new(options)

      self.new(
        :task_id        => master_job.task_id,
        :data           => nil,
        :process        => master_job.to_h,
        :map_or_reduce  => :master,
        :name           => master_job.name,
        :result_timeout => master_job.master_timeout,
        :retry          => master_job.master_retry || Skynet::CONFIG[:DEFAULT_MASTER_RETRY]
      )
    end

    def initialize(opts = {})
      unless opts[:task_id] and opts[:process] and opts[:map_or_reduce]
        raise ConstructorError.new("Must provide task_id, process and map_or_reduce")
      end
      @marshalable    = true
      @task_id        = opts[:task_id].to_i
      @data           = opts[:data]
      self.process    = opts[:process]
      @name           = opts[:name]
      @map_or_reduce  = opts[:map_or_reduce]
      @result_timeout = opts[:result_timeout]
      @retry          = opts[:retry]
    end

    def process=(process)
      if process.is_a?(Proc)
        @marshalable = false
      end
      @process = process
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

    def run(iteration=nil)
      info "running task #{name} TIMEOUT: #{result_timeout} task_id:#{task_id} MorR:#{map_or_reduce} PROCESS CLASS: #{@process.class}"
      begin
        Timeout::timeout(@result_timeout) do
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
        end
      rescue Timeout::Error => e
        # ==========
        # = XXX NEWSFEED HACK
        # = I'm printing the data hash, but that hash has all this shit added to it after runing through newsfeed.
        # = It's actually nice to be able to see what was added, but sometimes its too much data.
        # = Though the handy part will be adding instrumentation to the event_hash and seeing it onyl during a timeout.
        # ==========

        if @data.is_a?(Array) and @data.first.is_a?(Hash)
          @data.each {|h|h.delete(:event_object)}
        end
        raise TimeoutError.new("TASK TIMED OUT! #{name} IT:[#{iteration}] timeout:#{@result_timeout} #{e.inspect} DATA: #{@data.inspect} #{e.backtrace.join("\n")}")

      # ==========
      # = XXX This rescue block is probably not necessary.  Just for debugging for now. =
      # ==========
      rescue Exception => e
        error "Error running task #{e.inspect} TASK:", self, e.backtrace.join("\n")
        raise e
      end
    end

  end  ## END class Task
end