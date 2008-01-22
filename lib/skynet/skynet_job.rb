class Skynet		
  # Skynet::Job is the main interface to Skynet.   You create a job object giving 
  # it the starting data (map_data), along with what class has the map/reduce
  # functions in it.   Even though Skynet is distributed, when you call #run on
  # a plain Skynet::Job, it will still block in your current process until it has completed
  # your task.   If you want to go on to do other things you'll want to pass :async => true
  # when creating a new job.  Then later call job.results to retrieve your results.
  #     
  # There are also many global configuration options which can be controlled through Skynet::CONFIG
  #
  # Example Usage:
  # 
  #   class MapReduceTest
  #   
  #     def self.run
  #       job = Skynet::Job.new(
  #         :mappers          => 2, 
  #         :reducers         => 1,
  #         :map_reduce_class => self,
  #         :map_data         => [OpenStruct.new({:created_by => 2}),OpenStruct.new({:created_by => 2}),OpenStruct.new({:created_by => 3})]
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
  class Job
    include SkynetDebugger
    include Skynet::GuidGenerator		

    class WorkerError < Skynet::Error; end

    class BadMapOrReduceError < Skynet::Error; end

    class Error < Skynet::Error; end

    @@worker_ver = nil

    FIELDS = [:queue_id, :mappers, :reducers, :silent, :name, :map_timeout, :map_data, :job_id,
              :reduce_timeout, :master_timeout, :map_name, :reduce_name,
              :master_result_timeout, :result_timeout, :start_after, :solo, :single, :version,
              :map, :map_partitioner, :reduce, :reduce_partition, :map_reduce_class,
              :master_retry, :map_retry, :reduce_retry,
              :keep_map_tasks, :keep_reduce_tasks,
              :local_master, :async
            ]                                                       

    FIELDS.each do |method| 
      if [:map_reduce_class, :version, :map, :reduce, :map_data, :start_after].include?(method)
        attr_reader method
      elsif [:master_retry, :map_retry, :reduce_retry,:keep_map_tasks, :keep_reduce_tasks].include?(method)
        attr_writer method
      else
        attr_accessor method
      end
    end        
    
    attr_accessor :use_local_queue
    
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
      :local_master          => true
    }
            
    def self.debug_class_desc
      "JOB"
    end

    # Most of the time you will merely call #new(options) and then #run on the returned object.
    #
    # Options are:
    # <tt>:local_master</tt> BOOL (DEFAULT true)
    #   By default, your Skynet::Job will act as the master for your map/reduce job, doling out
    #   tasks, waiting for other workers to complete and return their results and dealing with
    #   merging and partitioning the data.   If you call #run in async mode, another worker will handle 
    #   being the master for your job without blocking.  If you run :async => false, :local_master => false
    #   Skynet will let another worker be the master for your job, but will block waiting for the 
    #   final results.  The benefit of this is that if your process dies, the Job will continue to
    #   run remotely.
    #
    # <tt>:async</tt> BOOL (DEFAULT false)
    #   If you run in async mode, another worker will handle being the master for your job without blocking.
    #   You can not pass :local_master => true, :async => true since the only way to allow your
    #   job to run asyncronously is to have a remote_master.
    #
    # <tt>:map_data</tt>(Array or Enumerable)
    #    map_data should be an Array or Enumerable that data Skynet::Job will split up 
    #    and distribute among your workers.   You can stream data to Skynet::Job by passing
    #    an Enumerable that implements next or each.    
    #
    # <tt>:map_reduce_class</tt>(Class or Class Name)
    #   Skynet::Job will look for class methods named self.map, self.reduce, self.map_partitioner, 
    #   self.reduce_partition in your map_reduce_class.  The only method requires is self.map.
    #   Each of these methods must accept an array.  Examples above.
    #
    # <tt>:map</tt>(Class Name)
    #   You can pass a classname, or a proc.  If you pass a classname, Job will look for a method
    #   called self.map in that class.
    #   WARNING: Passing a proc does not work right now.
    #
    # <tt>:reduce</tt>(Class Name)
    #   You can pass a classname, or a proc.  If you pass a classname, Job will look for a method
    #   called self.reduce in that class.
    #   WARNING: Passing a proc does not work right now.
    #
    # <tt>:reduce_partition</tt>(Class Name)
    #   You can pass a classname, or a proc.  If you pass a classname, Job will look for a method
    #   called self.reduce_partition in that class.
    #   WARNING: Passing a proc does not work right now.
    #
    # <tt>:mappers</tt> Fixnum
    #   The number of mappers to partition map data for.
    #
    # <tt>:reducers</tt> Fixnum
    #   The number of reducers to partition the returned map_data for.
    #
    # <tt>:master_retry</tt> Fixnum
    #   If the master fails for any reason, how many times should it be retried?  You can also set
    #   Skynet::CONFIG[:DEFAULT_MASTER_RETRY] (DEFAULT 0)
    #
    # <tt>:map_retry</tt> Fixnum
    #   If a map task fails for any reason, how many times should it be retried?  You can also set
    #   Skynet::CONFIG[:DEFAULT_MAP_RETRY] (DEFAULT 3)
    #   
    # <tt>:reduce_retry</tt> Fixnum
    #   If a reduce task fails for any reason, how many times should it be retried?  You can also set
    #   Skynet::CONFIG[:DEFAULT_REDUCE_RETRY] (DEFAULT 3)
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
    # <tt>:start_after</tt> Time or Time.to_i
    #   Do not start job until :start_after has passed
    #
    # <tt>:queue</tt> String
    #   Which queue should this Job go in to?  The queue provided is merely used to
    #   determine the queue_id.
    #   Queues are defined in Skynet::CONFIG[:MESSAGE_QUEUES]
    #
    # <tt>:queue_id</tt> Fixnum (DEFAULT 0)
    #   Which queue should this Job go in to?  
    #   Queues are defined in Skynet::CONFIG[:MESSAGE_QUEUES]
    #
    # <tt>:solo</tt> BOOL
    #   One normally turns solo mode in in Skynet::Config using Skynet::CONFIG[:SOLO] = true
    #   In solo mode, Skynet jobs do not add items to a Skynet queue. Instead they do all
    #   work in place.  It's like a Skynet simulation mode.  It will complete all tasks
    #   without Skynet running.  Great for testing.  You can also wrap code blocks in
    #   Skynet.solo {} to run that code in solo mode.
    #
    # <tt>:version</tt> Fixnum
    #   If you do not provide a version the current worker version will be used.
    #   Skynet workers start at a specific version and only look for jobs that match that version.
    #   A worker will continue looking for jobs at that version until there are no more jobs left on
    #   the queue for that version.  At that time, the worker will check to see if there is a new version.
    #   If there is, it will restart itself at the new version (assuming you had already pushed code to
    #   said workers.)
    #   To retrieve the current version, set the current version or increment the current version, see
    #   Skynet::Job.set_worker_version, Skynet::Job.get_worker_version, Skynet::Job.increment_worker_version
    #
    # <tt>:name</tt>, <tt>:map_name</tt>, <tt>:reduce_name</tt>
    #   These name methods are merely for debugging while watching the Skynet logs or the Skynet queue.
    #   If you do not supply names, it will try and provide sensible ones based on your class names.
    #
    # <tt>:keep_map_tasks</tt> BOOL or Fixnum (DEFAULT 1)
    #   If true, the master will run the map_tasks locally.
    #   If a number is provided, the master will run the map_tasks locally if there are 
    #   LESS THAN OR EQUAL TO the number provided.
    #   You may also set Skynet::CONFIG[:DEFAILT_KEEP_MAP_TASKS] DEFAULT 1
    #
    # <tt>:keep_reduce_tasks</tt> BOOL or Fixnum (DEFAULT 1)
    #   If true, the master will run the reduce_tasks locally.
    #   If a number is provided, the master will run the reduce_tasks locally if there are 
    #   LESS THAN OR EQUAL TO the number provided.
    #   You may also set Skynet::CONFIG[:DEFAILT_REDUCVE_MAP_TASKS] DEFAULT 1
    def initialize(options = {})                            
      FIELDS.each do |field|
        if options.has_key?(field)
          self.send("#{field}=".to_sym,options[field])
        elsif Skynet::CONFIG[:JOB_DEFAULTS][field]
          self.send("#{field}=".to_sym,Skynet::CONFIG[:JOB_DEFAULTS][field])
        end              
        if options[:queue]
          raise Error.new("The provided queue (#{options[:queue]}) does not exist in Skynet::CONFIG[:MESSAGE_QUEUES]") unless Skynet::CONFIG[:MESSAGE_QUEUES].index(options[:queue])
          self.queue_id = Skynet::CONFIG[:MESSAGE_QUEUES].index(options[:queue])
        end
                
        # Backward compatability
        self.mappers  ||= options[:map_tasks]
        self.reducers ||= options[:reduce_tasks]        
      end                     

      raise Error.new("You can not run a local master in async mode.") if self.async and self.local_master

      @job_id = task_id
    end

    # Options are:
    # <tt>:local_master</tt> BOOL (DEFAULT true)
    #   By default, your Skynet::Job will act as the master for your map/reduce job, doling out
    #   tasks, waiting for other workers to complete and return their results and dealing with
    #   merging and partitioning the data.   If you run in async mode, another worker will handle 
    #   being the master for your job without blocking.  If you run :async => false, :local_master => false
    #   Skynet will let another worker be the master for your job, but will block waiting for the 
    #   final results.  The benefit of this is that if your process dies, the Job will continue to
    #   run remotely.
    #
    # <tt>:async</tt> BOOL (DEFAULT false)
    #   If you run in async mode, another worker will handle being the master for your job without blocking.
    #   You can not pass :local_master => true, :async => true since the only way to allow your
    #   job to run asyncronously is to have a remote_master.
    #
    #   You can pass any options you might pass to Skynet::Job.new.  Warning: Passing options to run
    #   will permanently change properties of the job.
    def run(options = {})
      FIELDS.each do |field|
        if options.has_key?(field)
          self.send("#{field}=".to_sym,options[field])
        end
      end
      raise Error.new("You can not run a local master in async mode.") if self.async and self.local_master
      
      info "RUN 1 BEGIN #{name}, job_id:#{job_id} async:#{async}, local_master: #{local_master}, master?: #{master?}"
      
      # run the master task if we're running async or local_master
      if master?
        master_enqueue
        # ====================================================================================
        # = FIXME  If async Return a handle to an object that can used to retrieve the results later.
        # ====================================================================================
        async? ? job_id : master_results
      else
        number_of_tasks_queued = self.map_enqueue
        map_results            = self.map_results(number_of_tasks_queued)
        return unless map_results

        partitioned_data       = self.partition_data(map_results)
        return unless partitioned_data
        number_of_tasks_queued = self.reduce_enqueue(partitioned_data)
        
        @results = self.reduce_results(number_of_tasks_queued)
      end
    end
    
    def master_enqueue
      self.use_local_queue = local_master?
      messages = tasks_to_messages([master_task])
      enqueue_messages(messages)
    end
    
    # Returns the final results of this map/reduce job.  If results is called on an :async job
    # calling results will block until results are found or the master_timeout is reached.
    def results
      # ============================================
      # = FIXME Maybe this can have better warnings if the results aren't ready yet. =
      # ============================================
      master_results
    end
    
    def master_results                     
      @results ||= gather_results(1,master_timeout,name)
    end
    
    def map_enqueue      
      task_ids             = []
      map_tasks            = self.map_tasks
      self.use_local_queue = map_local?
      if map_tasks        
        number_of_tasks = 0
        map_tasks.each do |task|
          number_of_tasks += 1
          enqueue_messages(tasks_to_messages(task))
        end            
      end
      return number_of_tasks
    end
    
    def map_results(number_of_tasks)
      results = gather_results(number_of_tasks, map_timeout, map_name)
      return unless results
      debug "RUN MAP 2.5 RESULTS AFTER RUN #{display_info} results:", results.inspect
      results.compact! if results.is_a?(Array)
      results
    end

    def partition_data(post_map_data)
      debug "RUN REDUCE 3.1 BEFORE PARTITION #{display_info} reducers: #{reducers}"
      debug "RUN REDUCE 3.1 : #{reducers} #{name}, job_id:#{job_id}", post_map_data  
      partitioned_data = nil
      if not @reduce_partition
        # =====================
        # = XXX HACK
        # = There was a bug in Job where the reduce_partition of master jobs wasn't being set!  This is to catch that.
        # = It handles it by checking if the map class has a reduce partitioner.  Maybe this is a good thing to leave anyway.
        # =====================
        if @map.is_a?(String) and @map.constantize.respond_to?(:reduce_partition)
          partitioned_data = @map.constantize.reduce_partition(post_map_data, reducers)
        else
          partitioned_data = Skynet::Partitioners::RecombineAndSplit.reduce_partition(post_map_data, reducers)
        end
      elsif @reduce_partition.is_a?(String)
        partitioned_data = @reduce_partition.constantize.reduce_partition(post_map_data, reducers)
      else
        partitioned_data = @reduce_partition.call(post_map_data, reducers)
      end
      partitioned_data.compact!
      debug "RUN REDUCE 3.2 AFTER PARTITION #{display_info} reducers: #{partitioned_data.length}"
      debug "RUN REDUCE 3.2 AFTER PARTITION  #{display_info} data:", partitioned_data
      partitioned_data
    end

    def reduce_enqueue(partitioned_data)    
      return partitioned_data unless @reduce and reducers and reducers > 0
      debug "RUN REDUCE 3.3 CREATED REDUCE TASKS #{display_info}", partitioned_data
      
      reduce_tasks = self.reduce_tasks(partitioned_data)
      self.use_local_queue = reduce_local?(reduce_tasks)
      number_of_tasks = 0
      reduce_tasks.each do |task|
        number_of_tasks += 1
        enqueue_messages(tasks_to_messages(task))
      end            
      return number_of_tasks
    end
    
    def reduce_results(number_of_tasks)
      results = gather_results(number_of_tasks, reduce_timeout, reduce_name)
      if results.is_a?(Array) and results.first.is_a?(Hash)
        hash_results = Hash.new
        results.each {|h| hash_results.merge!(h) if h.class == Hash}
        results = hash_results
      elsif results.is_a?(Array) and results.first.is_a?(Array)
        results = results.compact      
      end
      debug "RUN REDUCE 3.4 AFTER REDUCE #{display_info} results size: #{results ? results.size : ''}"
      debug "RUN REDUCE 3.4 AFTER REDUCE #{display_info} results:", results if results
      return results
    end

    def enqueue_messages(messages)
      messages.each do |message|
        timeout = message.expiry || 5
        debug "RUN TASKS SUBMITTING #{message.name} job_id: #{job_id} #{message.payload.is_a?(Skynet::Task) ? 'task' + message.payload.task_id.to_s : ''}"        
        debug "RUN TASKS WORKER MESSAGE #{message.name} job_id: #{job_id}", message.to_a
        mq.write_message(message,timeout * 5)
      end
    end

    def gather_results(number_of_tasks, timeout=nil, description=nil)
      debug "GATHER RESULTS job_id: #{job_id} - NOT AN ASYNC JOB"
      results = {}
      errors  = {}
      started_at = Time.now.to_i

      begin
        loop do
          # debug "LOOKING FOR RESULT MESSAGE TEMPLATE"
          result_message = self.mq.take_result(job_id,timeout * 2)
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
        local_mq.reset! if use_local_queue?
        error "A WORKER EXPIRED or ERRORED, #{description}, job_id: #{job_id}"
        if not errors.empty?
          raise WorkerError.new("WORKER ERROR #{description}, job_id: #{job_id} errors:#{errors.keys.size} out of #{number_of_tasks} workers. #{errors.pretty_print_inspect}")
        else
          raise Skynet::RequestExpiredError.new("WORKER ERROR, A WORKER EXPIRED!  Did not get results or even errors back from all workers!")
        end
      end
      local_mq.reset! if use_local_queue?

      # ==========
      # = FIXME Tricky one.  Should we throw an exception if we didn't get all the results back, or should we keep going.
      # = Maybe this is another needed option.
      # ==========      
      # if not (errors.keys - results.keys).empty?
      #   raise WorkerError.new("WORKER ERROR #{description}, job_id: #{job_id} errors:#{errors.keys.size} out of #{number_of_tasks} workers. #{errors.pretty_print_inspect}")
      # end
      
      return nil if results.values.compact.empty?            
      return results.values
    end

    def master_task
      @master_task ||= begin    
        raise Exception.new("No map provided") unless @map

        # Make sure to set single to false in our own Job object.  
        # We're just passing along whether they set us to single.
        # If we were single, we'd never send off the master to be run externally.
        @single = false
        
        task = Skynet::Task.master_task(self)
      end
    end
	  
    def map_tasks
      @map_tasks ||= begin
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
            Skynet::Partitioners::SimplePartitionData.reduce_partition(@map_data, num_mappers)
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
    end
      
    def reduce_tasks(partitioned_data)
      @reduce_tasks ||= begin
        task_options = {
          :name           => reduce_name,
          :process        => @reduce,
          :map_or_reduce  => :reduce,
          :result_timeout => reduce_timeout,
          :retry          => reduce_retry || Skynet::CONFIG[:DEFAULT_REDUCE_RETRY]
        }
        Skynet::TaskIterator.new(task_options, partitioned_data)
      end
    end

		def tasks_to_messages(tasks)
      if tasks.is_a?(Skynet::TaskIterator)
        tasks = tasks.to_a
      elsif not tasks.is_a?(Array)
        tasks = [tasks]
      end

      tasks.collect do |task|
        Skynet::Message.new_task_message(task,self)
      end
	  end

    def master_retry
      @master_retry      || Skynet::CONFIG[:DEFAULT_MASTER_RETRY]
    end
    
    def map_retry
      @map_retry         || Skynet::CONFIG[:DEFAULT_MAP_RETRY]
    end
    
    def reduce_retry
      @reduce_retry      || Skynet::CONFIG[:DEFAULT_REDUCE_RETRY]
    end

    def keep_map_tasks
      @keep_map_tasks    || Skynet::CONFIG[:DEFAULT_KEEP_MAP_TASKS]
    end
    
    def keep_reduce_tasks
      @keep_reduce_tasks || Skynet::CONFIG[:DEFAULT_KEEP_REDUCE_TASKS]
    end
      
	  def map_local?
      return true if solo? or single?
      return true if keep_map_tasks == true
      return true if keep_map_tasks and map_tasks.data.is_a?(Array) and map_tasks.data.size <= keep_map_tasks
      return false
    end

	  def reduce_local?(reduce_tasks)
      return true if solo? or single?
      return true if keep_reduce_tasks == true
      return true if keep_reduce_tasks and reduce_tasks.data.is_a?(Array) and reduce_tasks.data.size <= keep_reduce_tasks
      return false
    end

    def use_local_queue?
      @use_local_queue
    end
    
    # async is true if the async flag is set and the job is not a 'single' job, or in solo mode.
    # async only applies to whether we run the master locally and whether we poll for the result
    def async?
      @async and not (solo? or single? or local_master?)
    end
        
    def master?
      async? or not local_master?
    end

    def local_master?
      @local_master or solo?
    end

    def solo?
      (@solo or CONFIG[:SOLO])
    end      

    def single?
      @single
    end

    def reset!
      @map_tasks    = nil
      @reduce_tasks = nil
    end

    def to_h
      if @map.kind_of?(Proc) or @reduce.kind_of?(Proc)
        raise Skynet::Error.new("You have a Proc in your map or reduce. This can't be turned into a hash.")
      end
      hash = {}
      FIELDS.each do |field|      
        hash[field] = self.send(field) if self.send(field)
      end       
      hash
    end
            
    def task_id
      @task_id ||= get_unique_id(1).to_i
    end

    def version
      return 1 if solo?
      @version ||= begin
        @@worker_version ||= self.mq.get_worker_version || 1
        @@worker_version
      end
    end
    
    def version=(v)
      @version = v
    end
    
    def display_info
      "#{name}, job_id: #{job_id}"
    end    

    def increment_worker_version
      newver = self.mq.get_worker_version + 1
      self.mq.set_worker_version(newver)
      newver
    end    

    def start_after=(time)
      @start_after = (time.is_a?(Time) ? time.to_i : time)
    end
    
    def map_data=(map_data)
      reset!
      @map_data = map_data
    end
        
    def map=(map)
      reset!
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
      reset!
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
      reset!
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
      @reduce_partitioner ||= klass if klass.constantize.respond_to?(:reduce_partition)
      @map_partitioner    ||= klass if klass.constantize.respond_to?(:map_partitioner)
    end

    def run_master
      error "run_master has been deprecated, please use run"
      run(:local_master => false)
    end
    
    def mq
      if use_local_queue?
        local_mq
      else
        @mq ||= Skynet::MessageQueue.new
      end
    end
    
    def local_mq
      @local_mq ||= LocalMessageQueue.new
    end

    def self.mq
      Skynet::MessageQueue.new
    end
    
    def self.set_worker_version(version)
      mq.set_worker_version(version)
    end
    
    def self.get_worker_version
      mq.get_worker_version
    end
    
    def self.increment_worker_version
      mq.increment_worker_version
    end
        
  end ### END class Skynet::Job 
end  

class Skynet::AsyncJob < Skynet::Job           
 # Skynet::AsyncJob is for Skynet jobs you want to run asyncronously.
 # Normally when you run a Skynet::Job it blocks until the job is complete.
 # Running an Async job merely returns a job_id which can be used later to retrieve the results.
 # See Skynet::Job for full documentation
  
  def initialize(options = {})
    options[:async]        = true
    options[:local_master] = false
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
end # class Skynet::AsyncJob   

class Skynet::Job::LocalMessageQueue
  include SkynetDebugger
  
  attr_reader :messages, :results

  def initialize
    @messages    = []
    @results     = []
  end
  
  def get_worker_version
    1
  end

  def take_result(job_id,timeout=nil)
    raise Skynet::RequestExpiredError.new if @messages.empty?
    run_message(@messages.shift)
  end

  def write_message(message,timeout=nil)
    @messages << message
  end
  
  def empty?
    @messages.empty?
  end

  def in_use?
    (not empty?)
  end
  
  def reset!
    @messages = []
    @results  = []
  end
  
  def run_message(message)
    result = nil
    (message.retry + 1).times do
      task = message.payload
      debug "RUN TASKS LOCALLY SUBMITTING #{message.name} task #{task.task_id}"        
      begin
        result = task.run
        break
      rescue Skynet::Task::TimeoutError => e
        result = e
        error "Skynet::Job::LocalMessageQueue Task timed out while executing #{e.inspect} #{e.backtrace.join("\n")}"
        next
      rescue Exception => e
        error "Skynet::Job::LocalMessageQueue :#{__LINE__} #{e.inspect} #{e.backtrace.join("\n")}"
        result = e
        next
      end
    end
    message.result_message(result)
  end
end # class LocalMessageQueue

class Skynet::TaskIterator
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
    data.send(each_method) do |task_data|
      task = nil
      if @first and iteration == 0
        task = @first
      else
        task = Skynet::Task.new(task_options.merge(:data => task_data, :task_id => (get_unique_id(1).to_i)))
        @first = task if iteration == 0
      end
      iteration += 1
      yield task
    end
  end      
end # class TaskIterator  


# require 'ruby2ruby'   # XXX this will break unless people have the fix to Ruby2Ruby
##### ruby2ruby fix from ruby2ruby.rb ############
### XXX This is bad.  Some people rely on an exception being thrown if a method is missing! BULLSHIT!
# class NilClass # Objective-C trick
  # def method_missing(msg, *args, &block)
  #   nil
  # end
# end
##############################
