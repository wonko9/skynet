class Skynet		
  class Job
    include SkynetDebugger
    include Skynet::GuidGenerator		

    class WorkerError < Skynet::Error
    end

    class BadMapOrReduceError < Skynet::Error
    end

    class Error < Skynet::Error
    end

		@@svn_rev = nil
    @@worker_ver = nil
    @@log = nil

    FIELDS = [:queue_id, :mappers, :reducers, :silent, :name, :map_timeout, :map_data, :job_id,
              :reduce_timeout, :master_timeout, :master, :map_name, :reduce_name, :async,
              :master_result_timeout, :result_timeout, :start_after, :solo, :single, :version,
              :map, :map_partitioner, :reduce, :reduce_partitioner, :map_reduce_class,
              :master_retry, :map_retry, :reduce_retry,
              :keep_map_tasks, :keep_reduce_tasks              
            ]                                                       

    FIELDS.each do |method| 
      # FIXME: use include?
      if method == :map_reduce_class or method == :version or method == :map or method == :reduce
        attr_reader method
      else
        attr_accessor method
      end
    end        

    Skynet::CONFIG[:JOB_DEFAULTS] = {
      :queue_id              => 0,
      :mappers               => 2,
      :reducers              => 1,               
      :map_timeout           => 60,
      :reduce_timeout        => 60,
      :master_timeout        => 60,
      :result_timeout        => 1200,
      :start_after           => 0,
      :master_result_timeout => 1200,
      :keep_map_tasks        => Skynet::CONFIG[:KEEP_MAP_TASKS],
      :keep_reduce_tasks     => Skynet::CONFIG[:KEEP_REDUCE_TASKS]
    }
            
    def self.debug_class_desc
      "JOB"
    end

    # Skynet::Job is the main interface to Skynet.   You create a job object giving 
    # it the starting data (map_data), along with what class has the map/reduce
    # functions in it.   Even though Skynet is distributed, when you call run on
    # a plain Skynet::Job, it will still block in your current process until it has completed
    # your task.   If you want to go on to do other things you'll want to use Skynet::AsyncJob
    # instead.  Skynet::AsyncJob has the same interface as Skynet::Job except you can call
    # run_master (synonym for run) which will immediately return to you a job_id which you
    # can later use to look up your result, if you care to see it.
    #     
    # There are many global configuration options which can be controlled through Skynet::CONFIG
    #
    # Example Usage:
    # 
    #   class MapReduceTest
    #   
    #     def self.run
    #       job = Skynet::Job.new(
    #         :mappers => 2, 
    #         :reducers => 1,
    #         :map_reduce_class => self,
    #         :map_data => [OpenStruct.new({:created_by => 2}),OpenStruct.new({:created_by => 2}),OpenStruct.new({:created_by => 3})]
    #       )    
    #       results = job.run
    #     end
    #   
    #     def self.map(profiles)
    #       result = Array.new
    #       profiles.each do |profile|
    #         result << [profile.created_by, 1] if profile.created_by
    #       end
    #       result
    #     end
    #   
    #     def self.reduce(pairs)
    #       totals = Hash.new
    #       pairs.each do |pair|
    #         created_by, count = pair[0], pair[1]
    #         totals[created_by] ||= 0
    #         totals[created_by] += count
    #       end
    #       totals
    #     end
    #   end
    # 
    #   MapReduceTest.run
    #
    # There are many other options to control various defaults and timeouts.
    #
    # Options are:
    # <tt>:map_data</tt>
    #    map_data is an ARRAY of data Skynet::Job will split up and distribute among
    #    your workers.  Even if your map_data is a single element, it must be passed as
    #    a single element array.
    #
    # <tt>:map_reduce_class</tt>
    #   Skynet::Job will look for class methods named self.map, self.reduce, self.map_partitioner, 
    #   self.reduce_partitioner in your map_reduce_class.  The only method requires is self.map.
    #   Each of these methods must accept an array.  Examples above.
    #
    # <tt>:name</tt>, <tt>:map_name</tt>, <tt>:reduce_name</tt>
    #   These name methods are merely for debugging while watching the Skynet logs or the Skynet queue.
    #   If you do not supply names, it will try and provide sensible ones based on your class names.
    #
    # <tt>:master</tt> BOOL
    #   Normally, skynet does all the work of distributing the map and reduce tasks in your current process.  
    #   That is to say, the process that's holding that job object.  If you create a master job or use
    #   run_master instead of just run it will turn the whole job itself into a task which will be picked
    #   up by a worker.  That worker will then distribute the tasks for you.   All Skynet::AsyncJob jobs are
    #   masters jobs.
    #
    # <tt>:master_timeout</tt>, <tt>:map_timeout</tt>, <tt>:reduce_timeout</tt>, <tt>master_result_timeout</tt>, <tt>result_timeout</tt>
    #   These control how long skynet should wait for particular actions to be finished.  
    #   The master_timeout controls how long the master should wait for ALL map/reduce tasks ie. the entire job to finish.
    #   The master_result_timeout controls how long the final result should wait in the queue before being expired.
    #   The map and reduce timeouts control how long individual map and reduce tasks shoudl take.
    #
    # <tt>:single</tt> BOOL
    #   By default the master task distributes the map and reduce tasks to other workers.  
    #   In single mode the master will take care of the map and reduce tasks by itself.
    #   This is handy when you really want to just perform some single action asyncronously.
    #   In this case you're merely using Skynet to postpone some action. In single mode, the
    #   first worker that picks up your task will just complete it as opposed to trying to distribute
    #   it to another worker.
    #
    # <tt>:start_after</tt> Time
    #   Sometimes you may want to delay a task until after a certain time.
    #
    # <tt>:solo</tt> BOOL
    #   One normally turns solo mode in in Skynet::Config using Skynet::CONFIG[:SOLO] = true
    #   In solo mode, Skynet jobs do not add items to a Skynet queue. Instead they do all
    #   work in place.  It's like a Skynet simulation mode.  It will complete all tasks
    #   without Skynet running.  Great for testing.   
    #
    # <tt>:version</tt> Fixnum
    #   Skynet workers run at a specific version and only look for jobs with their correct version.
    #   If you do not provide a version the current version will be used.
    #
    # <tt>:mappers</tt> Fixnum
    #   The number of mappers to partition map data for.
    #
    # <tt>:reducers</tt> Fixnum
    #   The number of reducers to partition the returned map_data for.
    #
    # <tt>:keep_map_tasks</tt> BOOL or Fixnum
    #   If true, the master will run the map_tasks locally.
    #   If a number is provided, the master will run the map_tasks locally if there are LESS THAN OR EQUAL TO the number provided
    #
    # <tt>:keep_reduce_tasks</tt> BOOL or Fixnum
    #   If true, the master will run the reduce_tasks locally.
    #   If a number is provided, the master will run the reduce_tasks locally if there are LESS THAN OR EQUAL TO the number provided
    def initialize(options = {})                            
      FIELDS.each do |field|
        if options[field]
          self.send("#{field}=".to_sym,options[field])
        elsif Skynet::CONFIG[:JOB_DEFAULTS][field]
          self.send("#{field}=".to_sym,Skynet::CONFIG[:JOB_DEFAULTS][field])
        end              
        if options[:queue]
          raise Error.new("The provided queue (#{options[:queue]}) does not exist in Skynet::CONFIG[:MESSAGE_QUEUES]") unless Skynet::CONFIG[:MESSAGE_QUEUES].index(options[:queue])
          self.queue_id = Skynet::CONFIG[:MESSAGE_QUEUES].index(options[:queue])
        end
        
        # Backward compatability
        self.mappers ||= options[:map_tasks]
        self.reducers ||= options[:reduce_tasks]        
      end                     

      @job_id = task_id
    end

    def to_h
      if @map.kind_of?(Proc) or @reduce.kind_of?(Proc)
        raise Skynet::Error.new("You have a Proc in your map or reduce. This can't be turned into a hash.")
      end
      hash = {}
      FIELDS.each do |field|      
        next unless self.send(field)
        hash[field] = self.send(field)
      end       
      hash
    end
        
    def mq
      @mq ||= Skynet::MessageQueue.new
    end
    
    ## set_version was supposed to know when to upgrade the version.  Haven't figured out how to do this yet
    def set_version
      true
      # return 1 if solo?
      # oldver = mq.get_worker_version || 0
      # if oldver != self.version
      #   mq.set_worker_version(self.version) 
      # end
    end

    def version
      return 1 if solo?
      @@worker_version ||= mq.get_worker_version
      @version ||= @@worker_version
    end
    
    def version=(v)
      @version = v
    end
    
    def display_info
      "#{name}, job_id: #{job_id}"
    end    

    def increment_worker_version
      newver = mq.get_worker_version + 1
      mq.set_worker_version(newver)
      newver
    end    

    def async?
      @async
    end
    
    def solo?
      (@solo or CONFIG[:SOLO])
    end      

    def single?
      @single
    end
    	  
    def run_tasks_and_gather_results(tasks, run_local=false)
      t1          = Time.now
      timeout     = tasks.first.result_timeout || 5
      description = tasks.first.name || "Generic Task"
      results     = nil

      # FIXME: solo mode
      run_tasks(tasks, run_local)

      # retrieve results unless async
      return true if async? or solo? or single? or run_local

      results = gather_results(job_id, tasks.size, timeout, description)
      info "RUN TASKS AND GATHER RESULTS COMPLETE #{description} RESULTS:#{ results ? results.size : 0 } jobid:#{job_id} TOOK: #{Time.now - t1}"
      return results
    end
    
    def run_tasks(tasks, run_local=false)
      tasks = if tasks.is_a?(Skynet::TaskIterator)
        tasks.to_a
      elsif not tasks.is_a?(Array)
        [tasks]
      end
      t1          = Time.now
      timeout     = tasks.first.result_timeout || 5
      description = tasks.first.name || "Generic Task"
    
      info "RUN TASKS #{description} LOCAL: #{run_local} ver: #{self.version} jobid: #{job_id} @ #{t1}: Q:#{queue_id}"
      if solo? or single? or run_local                   
        results = run_tasks_locally(tasks)      
      else
        enqueue_messages(messages_from_tasks(tasks))
      end
      return true
    end
    
    def enqueue_messages(messages)
      messages.each do |message|
        timeout = message.expiry || 5
        debug "RUN TASKS SUBMITTING #{message.name} job_id: #{job_id} #{message.payload.is_a?(Skynet::Task) ? 'task' + message.payload.task_id.to_s : ''}"        
        debug "RUN TASKS WORKER MESSAGE #{message.name} job_id: #{job_id}", message.to_a
        mq.write_message(message,timeout * 5)
      end
    end

    # XXX The problem with run_local is that it doesn't honor the master timeout, nor does it honor the retries.
    # It tries each task once and fails for that task if that task fails.  If the master would get retried
    # it would be fine, but you may have said that tasks get retried, but not masters.
    def run_tasks_locally(tasks)
      tasks       = tasks.to_a if tasks.is_a?(Skynet::TaskIterator)
      task_ids    = tasks.collect { | task | task.task_id }
      messages    = messages_from_tasks(tasks)
      description = tasks.first.name
      results     = {}
      errors      = {}
      messages.each do |message| 
        times_to_try = message.retry + 1
        times_to_try.times do
          task = message.payload
          debug "RUN TASKS SUBMITTING #{message.name} task #{task.task_id} job_id: #{job_id}"        
          begin
            results[task.task_id] = task.run
            break
          rescue Timeout::Error => e
            errors[task.task_id] = e
          rescue Exception => e
            errors[task.task_id] = e
          end
        end
      end                                                
      if not (task_ids - (results.keys + errors.keys)).empty?
        raise Skynet::Job::Error.new("WORKER ERROR #{description}, job_id: #{job_id} errors:#{errors.keys.size} out of #{task_ids.size} workers. #{errors.pretty_print_inspect}")
      end
      return nil if results.values.compact.empty?            
      return results.values
    end
    
    def gather_results(job_id, number_of_tasks, timeout=nil, description=nil)
      debug "GATHER RESULTS job_id: #{job_id} - NOT AN ASYNC JOB"
      results = {}
      errors  = {}
      started_at = Time.now.to_i
      begin
        loop do
          # debug "LOOKING FOR RESULT MESSAGE TEMPLATE"
          result_message = mq.take_result(job_id,timeout * 2)
          ret_result     = result_message.payload

          if result_message.payload_type == :error
            errors[result_message.task_id] = ret_result
            error "ERROR RESULT TASK #{result_message.task_id} returned #{errors[result_message.task_id].inspect}"
          else
            results[result_message.task_id] = ret_result
            debug "RESULT returned TASKID: #{result_message.task_id} #{results[result_message.task_id].inspect}"
          end          
          debug "RESULT collected: #{(results.keys + errors.keys).size}, remaining: #{(number_of_tasks - (results.keys + errors.keys).uniq.size)}"
          break if (number_of_tasks - (results.keys + errors.keys).uniq.size) <= 0
        end
      rescue Skynet::RequestExpiredError => e
        error "A WORKER EXPIRED or ERRORED, #{description}, job_id: #{job_id}"
        if not errors.empty?
          raise WorkerError.new("WORKER ERROR #{description}, job_id: #{job_id} errors:#{errors.keys.size} out of #{number_of_tasks} workers. #{errors.pretty_print_inspect}")
        else
          raise Skynet::RequestExpiredError.new("WORKER ERROR, A WORKER EXPIRED!  Did not get results or even errors back from all workers!")
        end
      end     
      return nil if results.values.compact.empty?            
      return results.values
    end

		def messages_from_tasks(tasks)
      tasks = tasks.to_a if tasks.is_a?(Skynet::TaskIterator)

      tasks.collect do |task|
        # FIXME: Add a named constuctor in message for making a message from a task.
        worker_message = Skynet::Message.new(
          :tasktype     => :task, 
          :job_id       => job_id,
          :task_id      => task.task_id,
          :payload      => task,
          :payload_type => task.task_or_master,
          :expiry       => task.result_timeout, 
          :expire_time  => @start_after,
          :iteration    => 0,
          :name         => task.name,       
          :version      => @version,
          :retry        => task.retry,
          :queue_id     => queue_id
        )
      end
	  end
        
    def map=(map)
      return unless map
      if map.class == String or map.class == Class
        @map = map.to_s
      elsif map.is_a?(Proc)
        @map = map        
      else
        raise BadMapOrReduceError.new("#{self.class}.map accepts a class name or a proc. Got #{map}") 
      end      
    end

    def reduce=(reduce)
      return unless reduce
      if reduce.class == String or reduce.class == Class
        @reduce = reduce.to_s
      elsif reduce.is_a?(Proc)
        @reduce = reduce        
      else
        raise BadMapOrReduceError.new("#{self.class}.reduce accepts a class name or a proc. Got #{reduce}") 
      end      
    end 

    def map_reduce_class=(klass)
      unless klass.class == String or klass.class == Class
        raise BadMapOrReduceError.new("#{self.class}.map_reduce only accepts a class name: #{klass} #{klass.class}") 
      end
      klass = klass.to_s
      @map = klass
      self.name     ||= "#{klass} MASTER"
      self.map_name ||= "#{klass} MAP"
      if klass.constantize.respond_to?(:reduce)
        @reduce ||= klass 
        self.reduce_name ||= "#{klass} REDUCE"
      end
      @reduce_partitioner ||= klass if klass.constantize.respond_to?(:reduce_partitioner)
      @map_partitioner    ||= klass if klass.constantize.respond_to?(:map_partitioner)
    end

    def task_id
      @task_id ||= get_unique_id(1).to_i
    end
  
    # Run this skynet job, returning the job_id once the job is queued.
    def run_master
      if solo?
        run_job
      else
        results = run_tasks_and_gather_results(master_task)
        if async?
          return self.job_id
        else
          return results
        end          
      end
    end

    def master_task
      @master_task ||= begin    
        raise Exception.new("No map provided") unless @map
        set_version
        options = {}
        FIELDS.each do |field|
          options[field] = begin
            case field
            when :async
              false
            when :map_name, :reduce_name
              self.send(field) || self.send(:name)
            else
              self.send(field)
            end  
          end
        end
        
        job = Skynet::Job.new(options)

        # Make sure to set single to false in our own Job object.  
        # We're just passing along whether they set us to single.
        # If we were single, we'd never send off the master to be run externally.
        @single = false
        
        # FIXME: Possibly add a named constructor in task for this? 
        task = Skynet::Task.new(
          :task_id        => task_id, 
          :data           => nil, 
          :process        => job.to_h, 
          :map_or_reduce  => :master,
          :name           => self.name,
          :result_timeout => master_timeout,
          :retry          => master_retry || Skynet::CONFIG[:DEFAULT_MASTER_RETRY]
        )
      end
    end
  
    # Run the job and return result arrays    
    def run
      if async?
        run_master
      else
        run_job
      end
    end

    def run_job
      debug "RUN 1 BEGIN #{name}, job_id:#{job_id}"
      set_version
      # unless (@map && @reduce)
      raise ArgumentError, "map lambdas not assigned" unless (@map)
    
      debug "RUN 2 MAP pre run_map #{name}, job_id:#{job_id}"

      post_map_data = run_map
      debug "RUN 3 REDUCE pre run_reduce #{name}, job_id:#{job_id}"
      return nil unless post_map_data
      results = run_reduce(post_map_data)
      debug "RUN 4 FINISHED run_job #{name}, job_id:#{job_id}"
      results
    end
  
    def map_tasks
      map_tasks = []
      debug "RUN MAP 2.1 #{display_info} data size before partition: #{@map_data.size}"
      debug "RUN MAP 2.1 #{display_info} data before partition:", @map_data

      task_options = {
        :process        => @map,
        :name           => map_name,
        :map_or_reduce  => :map,
        :result_timeout => map_timeout,
        :retry          => map_retry || Skynet::CONFIG[:DEFAULT_MAP_RETRY]
      }

      if @map_data.is_a?(Array)
        debug "RUN MAP 2.2 DATA IS Array #{display_info}"
        num_mappers = @map_data.length < @mappers ? @map_data.length : @mappers

        map_data = if @map_partitioner
          @map_partitioner.call(@map_data,num_mappers)
        else
          Skynet::Partitioners::SimplePartitionData.reduce_partitioner(@map_data, num_mappers)
        end

        debug "RUN MAP 2.3 #{display_info} data size after partition: #{map_data.size}"
        debug "RUN MAP 2.3 #{display_info} map data after partition:", map_data
      elsif @map_data.is_a?(Enumerable)
        debug "RUN MAP 2.2 DATA IS ENUMERABLE #{display_info} map_data_class: #{@map_data.class}"
        map_data = @map_data
      else
        debug "RUN MAP 2.2 DATA IS NOT ARRAY OR ENUMERABLE #{display_info} map_data_class: #{@map_data.class}"
        map_data = [ @map_data ]
      end
      Skynet::TaskIterator.new(task_options, map_data)
    end
    
    # Partition up starting data, create map tasks
    def run_map      
      post_map_data = nil
      begin                       
        map_tasks = self.map_tasks
        if map_tasks
          if keep_map_tasks == true or (map_tasks.data.is_a?(Array) and map_tasks.size <= keep_map_tasks)
            post_map_data = run_tasks_locally(map_tasks)
          else
            number_of_tasks = 0
            map_tasks.each do |task|
              number_of_tasks += 1
              run_tasks(task)
            end            
            post_map_data = gather_results(job_id, number_of_tasks, map_timeout, map_name)            
          end           
        end
      rescue WorkerError => e
        error "MAP FAILED #{display_info} #{e.class} #{e.message.inspect}"
        return nil
      end
    
      debug "RUN MAP 2.5 RESULTS AFTER RUN #{display_info} results:", post_map_data.inspect
      return nil unless post_map_data
      post_map_data.compact! if post_map_data.class == Array    
      return post_map_data
    end
  
    def reduce_tasks(post_map_data)
      debug "RUN REDUCE 3.1 BEFORE PARTITION #{display_info} reducers: #{reducers}"
      debug "RUN REDUCE 3.1 : #{reducers} #{name}, job_id:#{job_id}", post_map_data  
      reduce_data = run_reduce_partitioner(post_map_data, reducers)
      reduce_data.compact!
      debug "RUN REDUCE 3.2 AFTER PARTITION #{display_info} reducers: #{reduce_data.length}"
      debug "RUN REDUCE 3.2 AFTER PARTITION  #{display_info} data:", reduce_data

      reduce_tasks = (0..reduce_data.length - 1).collect do |i|
        Skynet::Task.new(
          :task_id        => get_unique_id(:no_disk).to_i,
          :data           => reduce_data[i], 
          :name           => reduce_name,
          :process        => @reduce,
          :map_or_reduce  => :reduce,
          :result_timeout => reduce_timeout,
          :retry          => reduce_retry || Skynet::CONFIG[:DEFAULT_REDUCE_RETRY]
        )
      end
      reduce_tasks.compact! if reduce_tasks      
      reduce_tasks
    end
    
    # Re-partition returning data for reduction, create reduce tasks
    def run_reduce(post_map_data=nil)    
      return post_map_data unless post_map_data and @reduce and reducers and reducers > 0
      debug "RUN REDUCE 3.3 CREATED REDUCE TASKS #{display_info}", post_map_data
      results = nil
      
      # Reduce and return results
      begin
        reduce_tasks = self.reduce_tasks(post_map_data)
        if keep_reduce_tasks == true or (reduce_tasks.size <= keep_reduce_tasks)
          run_local = true
        end       
        results = run_tasks_and_gather_results(reduce_tasks, run_local)
      rescue WorkerError => e
        error "REDUCE FAILED #{display_info} #{e.class} #{e.message.inspect}"
        return nil
      end

      if results.class == Array and results.first.class == Hash
        hash_results = Hash.new
        results.each {|h| hash_results.merge!(h) if h.class == Hash}
        # results.flatten! if results
        results = hash_results
      else
        results = []
      end
      debug "RUN REDUCE 3.4 AFTER REDUCE #{display_info} results size: #{results.size}"
      debug "RUN REDUCE 3.4 AFTER REDUCE #{display_info} results:", results
      return results
    end
  
    def run_reduce_partitioner(post_map_data,num_reducers)
      if not @reduce_partitioner
        # =====================
        # = XXX HACK
        # = There was a bug in Job where the reduce_partitioner of master jobs wasn't being set!  This is to catch that.
        # = It handles it by checking if the map class has a reduce partitioner.  Maybe this is a good thing to leave anyway.
        # =====================
        if @map.is_a?(String) and @map.constantize.respond_to?(:reduce_partitioner)
          @map.constantize.reduce_partitioner(post_map_data, num_reducers)
        else
          Skynet::Partitioners::RecombineAndSplit.reduce_partitioner(post_map_data, num_reducers)
        end
      elsif @reduce_partitioner.is_a?(String)
        @reduce_partitioner.constantize.reduce_partitioner(post_map_data, num_reducers)
      else
        @reduce_partitioner.call(post_map_data, num_reducers)
      end
    end
        
  end ### END class Skynet::Job 
  
  class AsyncJob < Skynet::Job           
   # Skynet::AsyncJob is for Skynet jobs you want to run asyncronously.
   # Normally when you run a Skynet::Job it blocks until the job is complete.
   # Running an Async job merely returns a job_id which can be used later to retrieve the results.
   # See Skynet::Job for full documentation
    
    def initialize(options = {})
      options[:async] = true
      super(options)      
    end


    def map=(klass)
      unless klass.class == String or klass.class == Class
        raise BadMapOrReduceError.new("#{self.class}.map only accepts a class name") 
      end
      @map = klass.to_s
    end

    def reduce=(klass)
      unless klass.class == String or klass.class == Class
        raise BadMapOrReduceError.new("#{self.class}.reduce only accepts a class name") 
      end
      @reduce = klass.to_s
    end 
        
    # Synonym for run_master
    def run
      if solo?
        super
      else
        run_master
      end
    end
        
  end ### END class Skynet::AsyncJob   

  class TaskIterator
    include SkynetDebugger
    include Skynet::GuidGenerator		
    
    class Error < StandardError
    end
    
    include Enumerable

    attr_accessor :task_options, :data

    def initialize(task_options, data)            
      @task_options = task_options
      @data         = data
    end


    def first         
      if data.respond_to?(:first)
        @first ||= Skynet::Task.new(task_options.merge(:data => data.first, :task_id => get_unique_id(1).to_i))        
      else
        raise Error.new("#{data.class} does not implement 'first'")
      end
    end

    def size         
      if data.respond_to?(:size)
        data.size
      else
        raise Error.new("#{data.class} does not implement 'size'")
      end
    end
    
    def [](index)
      if data.respond_to?(:[])
        Skynet::Task.new(task_options.merge(:data => data[index], :task_id => get_unique_id(1).to_i))
      else
        raise Error.new("#{data.class} does not implement '[]'")
      end
    end

    def each_method
      each_method = data.respond_to?(:next) ? :next : :each        
    end
    
    def to_a
      self.collect { |task| task }
    end
    
    def each     
      iteration = 0        
      start_task_id = get_unique_id(1).to_i
      data.send(each_method) do |task_data|
        task = nil
        if @first and iteration == 0
          task = @first
        else
          task = Skynet::Task.new(task_options.merge(:data => task_data, :task_id => (start_task_id + iteration)))
          @first = task if iteration == 0
        end
        iteration += 1
        yield task
      end
    end
  end

end

# require 'ruby2ruby'   # XXX this will break unless people have the fix to Ruby2Ruby
##### ruby2ruby fix from ruby2ruby.rb ############
### XXX This is bad.  Some people rely on an exception being thrown if a method is missing! BULLSHIT!
# class NilClass # Objective-C trick
  # def method_missing(msg, *args, &block)
  #   nil
  # end
# end
##############################
