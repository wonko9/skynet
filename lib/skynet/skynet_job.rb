#
# Copyright (c) 2006 Josh Carter <josh@multipart-mixed.com>
#
# All rights reserved.  You can redistribute and/or modify this under
# the same terms as Ruby.
#

# require 'rinda/ring'
# require 'skynet_message'
require 'pp'

# require 'ruby2ruby'   # XXX this will break unless people have the fix to Ruby2Ruby

##### ruby2ruby fix from ruby2ruby.rb ############
### XXX This is bad.  Some people rely on an exception being thrown if a method is missing! BULLSHIT!
# class NilClass # Objective-C trick
  # def method_missing(msg, *args, &block)
  #   nil
  # end
# end
##############################



# Users should create instances of this class. Rather than subclassing,
# jobs are specialized by assigning lambdas to map, reduce, and partition.
# This allows the instance to easily create sub-tasks and marshal the map
# and reduce code for sending to workers.
#


class Skynet
		
  class Job

    class WorkerError < Skynet::Error
    end

    class BadMapOrReduceError < Skynet::Error
    end

    class Error < Skynet::Error
    end

    def self.debug_class_desc
      "JOB"
    end

		@@svn_rev = nil
    @@worker_ver = nil

    include SkynetDebugger
    include Skynet::GuidGenerator

    attr_accessor :map_tasks, :reduce_tasks, :silent, :map_timeout, :map_data, :job_id
    attr_accessor :reduce_timeout, :master_timeout, :master, :async
    attr_accessor :master_result_timeout, :result_timeout
    attr_accessor :map, :map_partitioner, :reduce, :reduce_partitioner # User-provided lambdas
    attr_accessor :map_name, :name, :reduce_name

    # New job: caller specifies quantity of map and reduce tasks.
    #
		

    @@log = nil

    FIELDS = [:map_tasks, :reduce_tasks, :silent, :name, :map_timeout, :map_data, :job_id,
              :reduce_timeout, :master_timeout, :master, :map_name, :reduce_name, :async,
              :master_result_timeout, :result_timeout,
              :map, :map_partitioner, :reduce, :reduce_partitioner
            ]

    def initialize(opts = {})
      @map_tasks = opts[:map_tasks] || 2
      @reduce_tasks = opts[:reduce_tasks] || 1
      @silent = opts[:silent] || false
      @name = opts[:name] if opts[:name]
      @map_name = opts[:map_name] if opts[:map_name]
      @reduce_name = opts[:reduce_name] if opts[:reduce_name]
      @map_timeout = opts[:map_timeout] || 1.minute
      @reduce_timeout = opts[:reduce_timeout] || 1.minute
      @master_timeout = opts[:master_timeout] || 1.minute
      @master = opts[:master] || false
      @master_result_timeout = opts[:master_result_timeout] || 20.minutes
      @result_timeout = opts[:result_timeout] || 20.minutes
      
      @version = opts[:version] || nil
            
      @map_data = opts[:map_data]
			if opts[:map_reduce_class]
			  self.map_reduce_class = opts[:map_reduce_class]
		  else
		    self.map = opts[:map] if opts[:map]
		    self.reduce = opts[:reduce] if opts[:reduce]
	    end

      @async = opts[:async] || false
      @job_id = task_id
    end

    def to_h
      if @map.kind_of?(Proc) or @reduce.kind_of?(Proc)
        raise Skynet::Error.new("You have a Proc in your map or reduce. This can't be turned into a hash.")
      end
      hash = {}
      FIELDS.each do |field|      
        value = self.send(field)
        next unless value
        hash[field] = value
      end
      hash
    end
        
    def mq
      @mq ||= Skynet::MessageQueue.new
    end
    
    def set_version
      true
      # return 1 if CONFIG[:SOLO]
      # oldver = mq.get_worker_version || 0
      # if oldver != self.version
      #   mq.set_worker_version(self.version) 
      # end
    end

    def version
      return 1 if CONFIG[:SOLO]
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
				
    # submit jobs to Skynet::MessageQueue

    def run_tasks(tasks,timeout = 5,description = "Generic Task")
      result = Hash.new
      errors = Hash.new
			mq = Skynet::MessageQueue.new unless CONFIG[:SOLO]
      t1 = Time.now
      info "RUN TASKS #{description} ver: #{self.version} jobid: #{job_id} @ #{t1}"
      tasks = [tasks] unless tasks.class == Array

      # write tasks to the MessageQueue
      task_ids = []
      tasks.each do |task|
        debug "RUN TASKS SUBMITTING #{description} task #{task.task_id} job_id: #{job_id}"
        
        if CONFIG[:SOLO]
          result[task.task_id] = task.run
        else        
          task_ids << task.task_id
      
          worker_message = Skynet::Message.new(
            :tasktype=> :task, 
            :job_id => job_id,
            :task_id => task.task_id,
            :payload => task,
            :payload_type => task.task_or_master,
            :expiry => timeout, 
            :expire_time => 0,
            :iteration => 0,
            :name => description,       
            :version => @version
          )
          debug "RUN TASKS WORKER MESSAGE #{description} job_id: #{job_id}", worker_message.to_a
          mq.write_message(worker_message,timeout * 5)
        end
      end

      return result.values if CONFIG[:SOLO]
    
      return true if async
    
      debug "GATHER RESULTS for #{description} job_id: #{job_id} - NOT AN ASYNC JOB"
    
      # retrieve results unless async
      begin
        loop do
          # debug "LOOKING FOR RESULT MESSAGE TEMPLATE"
          result_message = mq.take_result(job_id,timeout * 2)

          ret_result = result_message.payload
          if result_message.payload_type == :error
            errors[result_message.task_id] = ret_result
            error "ERROR RESULT TASK #{result_message.task_id} returned #{errors[result_message.task_id].inspect}"
          else
            result[result_message.task_id] = ret_result
            debug "RESULT returned TASKID: #{result_message.task_id} #{result[result_message.task_id].inspect}"
          end          
          debug "RESULT collected: #{(result.keys + errors.keys).size}, remaining: #{(task_ids - (result.keys + errors.keys)).size}"
          break if (task_ids - (result.keys + errors.keys)).empty?
          if (task_ids - (result.keys & errors.keys)).empty?
            raise Skynet::Job::Error.new("WORKER ERROR #{description}, job_id: #{job_id} errors:#{errors.keys.size} out of #{task_ids.size} workers. #{errors.pretty_print_inspect}")
          end
        end
      rescue Skynet::RequestExpiredError => e 
        error "A WORKER EXPIRED or ERRORED, #{description}, job_id: #{job_id}"
        if not errors.empty?
          raise WorkerError.new("WORKER ERROR #{description}, job_id: #{job_id} errors:#{errors.keys.size} out of #{task_ids.size} workers. #{errors.pretty_print_inspect}")
        else
          raise Skynet::RequestExpiredError.new("WORKER ERROR, A WORKER EXPIRED!  Did not get results or even errors back from all workers!")
        end
      end
      info "RUN TASKS COMPLETE #{description} jobid: #{job_id} TOOK: #{Time.now - t1}"
      result.values      
    end 
        
    def map_reduce_class=(klass)
      unless klass.class == String or klass.class == Class
        raise BadMapOrReduceError.new("#{self.class}.map_reduce only accepts a class name") 
      end
      klass = klass.to_s
      @map = klass
      self.name ||= "#{klass} MASTER"
      self.map_name ||= "#{klass} MAP"
      if klass.constantize.respond_to?(:reduce)
        @reduce = klass 
        self.reduce_name ||= "#{klass} REDUCE"
      end
      @reduce_partitioner = klass if klass.constantize.respond_to?(:reduce_partitioner)
      @map_partitioner = klass if klass.constantize.respond_to?(:map_partitioner)
    end

    # def args_pp(*args)
    #   "#{args.length > 0 ? args.pretty_print_inspect : ''}"
    # end
    #   
    # def debug(msg,*args)
    #   log = Skynet::Logger.get
    #   log.debug "#{self.class} DEBUG: [#{self.name}] #{msg} #{args_pp(*args)}"
    # end
    # 
    # def info(msg, *args)
    #   log = Skynet::Logger.get
    #   log.info "#{self.class} #{msg} #{args_pp(*args)}"
    # end
    # 
    # def warn(msg, *args)
    #   log = Skynet::Logger.get
    #   log.warn "#{self.class} #{msg} #{args_pp(*args)}"
    # end
    #   
    # def error(msg, *args)
    #   log = Skynet::Logger.get
    #   log.error "#{self.class} #{msg} #{args_pp(*args)}"
    # end
    # 
    # def fatal(msg, *args)
    #   log = Skynet::Logger.get
    #   log.fatal "#{self.class} #{msg} #{args_pp(*args)}"
    # end

    def task_id
      @task_id ||= get_unique_id(1)
    end
  
    def run_master
      result =  run_tasks(master_task,master_timeout,name)
      debug "MASTER RESULT #{self.name} job_id: #{self.job_id}", result
      result
    end
  
    # def async_master_task
    #   @async_master_task ||= begin    
    #     raise Exception.new "No map provided" unless @map
    #     set_version
    #     # process = <<-CODE
    #     #   job = Skynet::Job.new
    #     #   job.map_timeout = #{map_timeout}
    #     #   job.reduce_timeout = #{reduce_timeout}
    #     #   job.map_data = #{Marshal.dump(@map_data)}
    #     #   job.map_name = '#{map_name}'
    #     #   job.reduce_name = '#{reduce_name}'
    #     #   job.map = '#{@map ? @map.to_ruby : nil}'
    #     #   job.map_partitioner = '#{@map_partitioner ? @map_partitioner.to_ruby : nil}'
    #     #   job.reduce = '#{@reduce ? @reduce.to_ruby : nil}'
    #     #   job.reduce_partitioner = '#{@reduce_partitioner ? @reduce_partitioner.to_ruby : nil}'
    #     #   job.map_tasks = #{@map_tasks}
    #     #   job.reduce_tasks = #{@reduce_tasks}
    #     #   job.name = '#{@name}'
    #     #   puts "RUNNING MASTER RUN"
    #     #   job.run          
    #     # CODE
    #     # pp "PROCESS",process
    #     task = Skynet::Task.new(task_id, :master, map, :map)
    #     task.name = self.name
    #     # begin
    #     #   Marshal.dump(task)
    #     # rescue Exception => e
    #     #   debug "Cant pass object by value"
    #     # end
    #     task
    #   end
    # end
    # 

    def master_task
      @master_task ||= begin    
        raise Exception.new("No map provided") unless @map
        set_version
        job = Skynet::Job.new
        job.map_timeout = map_timeout
        job.reduce_timeout = reduce_timeout
        job.job_id = job.task_id
        job.map_data = @map_data
        job.map_name = map_name || name
        job.reduce_name = reduce_name || name
        job.map = @map
        job.map_partitioner = @map_partitioner
        job.reduce = @reduce
        job.reduce_partitioner = @reduce_partitioner
        job.map_tasks = @map_tasks
        job.reduce_tasks = @reduce_tasks
        job.name = @name
        job.version = version
        process = lambda do |data|
          debug "RUNNING MASTER RUN #{name}, job_id:#{job_id}"
          job.run
        end
        task = Skynet::Task.new(
          :task_id => task_id, 
          :data => :master, 
          :process => process, 
          :map_or_reduce => :master,
          :name => self.name,
          :result_timeout => master_result_timeout
        )
      end
    end
  
    # Run the job and return result arrays
    #
    
    def run
      run_job
    end

    def run_job
      debug "RUN 1 BEGIN #{name}, job_id:#{job_id}"
      set_version
      # unless (@map && @reduce)
      unless (@map)
        raise ArgumentError, "map lambdas not assigned"
      end
    
      # sometimes people want to run a master with just run.  In this case we assume we have to set the data to the map_data
      # XXX seems like a hack

      # if (@reduce and not @reduce_partitioner)
      #   debug "SHIT BAD REDUCE"
      #   raise ArgumentError, "reduce lambdas assigned without partition lambda"
      # end

      debug "RUN 2 MAP pre run_map #{name}, job_id:#{job_id}"

      post_map_data = run_map
      debug "RUN 3 REDUCE pre run_reduce #{name}, job_id:#{job_id}"
      return post_map_data unless post_map_data
      results = run_reduce(post_map_data)
      debug "RUN 4 FINISHED run_job #{name}, job_id:#{job_id}"
			results
    end
  
    def run_map
      # Partition up starting data, create map tasks
      #
      map_tasks = Array.new
      debug "RUN MAP 2.1 #{display_info} data size before partition: #{@map_data.size}"
      debug "RUN MAP 2.1 #{display_info} data before partition:", @map_data
      if @map_data.class == Array
        debug "RUN MAP 2.2 DATA IS Array #{display_info}"
        num_mappers = @map_data.length < @map_tasks ? @map_data.length : @map_tasks
        pre_map_data = Array.new
        if @map_partitioner
          pre_map_data = @map_partitioner.call(@map_data,num_mappers)
        else
          pre_map_data = Partitioner::simple_partition_data(@map_data, num_mappers)
        end
        debug "RUN MAP 2.3 #{display_info} data size after partition: #{pre_map_data.size}"
        debug "RUN MAP 2.3 #{display_info} map data after partition:", pre_map_data
        map_tasks = Array.new

        (0..num_mappers - 1).each do |i|
          map_tasks << Skynet::Task.new(
                        :task_id => get_unique_id(1), 
                        :data => pre_map_data[i], 
                        :process => @map,
                        :name => map_name,
                        :map_or_reduce => :map,
                        :result_timeout => result_timeout
                      )
        end

        # Run map tasks
        #
      elsif @map_data.is_a?(Enumerable)
        debug "RUN MAP 2.2 DATA IS ENUMERABLE #{display_info} map_data_class: #{@map_data.class}"
        each_method = @map_data.respond_to?(:next) ? :next : :each
        @map_data.send(each_method) do |pre_map_data|
          map_tasks << Skynet::Task.new(
                        :task_id => get_unique_id(1), 
                        :data => pre_map_data, 
                        :process => @map,
                        :name => map_name,
                        :map_or_reduce => :map,
                        :result_timeout => result_timeout
                      )
        end
      else
        debug "RUN MAP 2.2 DATA IS NOT ARRAY OR ENUMERABLE #{display_info} map_data_class: #{@map_data.class}"
        map_tasks = [ 
          Skynet::Task.new(
            :task_id => get_unique_id(1), 
            :data => @map_data, 
            :process => @map, 
            :name => map_name,
            :map_or_reduce => :map, 
            :result_timeout => result_timeout
          ) 
        ]
      end      

      begin
        map_data = run_tasks(map_tasks,map_timeout,map_name)
      rescue WorkerError => e
        error "MAP FAILED #{display_info} #{e.class} #{e.message.inspect}"
        return nil
      end
    
      return nil unless map_data

      map_data.compact! if map_data.class == Array
    
      debug "RUN MAP 2.5 RESULTS AFTER RUN #{display_info} results:", map_data.inspect
      return map_data
    end
  
    def run_reduce(map_data=nil)    
      return map_data unless map_data and @reduce
      # Re-partition returning data for reduction, create reduce tasks
      #
    
      num_reducers = @reduce_tasks

      debug "RUN REDUCE 3.1 BEFORE PARTITION #{display_info} num_reducers: #{num_reducers}"
      # debug "RUN REDUCE 3.1 : #{num_reducers} #{name}, job_id:#{job_id}", map_data
  
      reduce_data = run_reduce_partitioner(map_data, num_reducers)
      reduce_data.compact!
      debug "RUN REDUCE 3.2 AFTER PARTITION #{display_info} num_reducers: #{reduce_data.length}"
      debug "RUN REDUCE 3.2 AFTER PARTITION  #{display_info} data:", reduce_data
      reduce_tasks = Array.new

      (0..reduce_data.length - 1).each do |i|
        reduce_tasks << Skynet::Task.new(
                      :task_id => get_unique_id(1), 
                      :data => reduce_data[i], 
                      :name => reduce_name,
                      :process => @reduce,
                      :map_or_reduce => :reduce,
                      :result_timeout => result_timeout
                    )
      end
      reduce_tasks.compact! if reduce_tasks      
    
      debug "RUN REDUCE 3.3 CREATED REDUCE TASKS #{display_info}"#, reduce_tasks
  
      # Reduce and return results
      #
      begin
        results = run_tasks(reduce_tasks, reduce_timeout,reduce_name)
      rescue WorkerError => e
        error "REDUCE FAILED #{display_info} #{e.class} #{e.message.inspect}"
        return nil
      end

      if results.class == Array and results.first.class == Hash
        hash_results = Hash.new
        results.each {|h| hash_results.merge!(h) if h.class == Hash}
        # results.flatten! if results
        results = hash_results
			end
      debug "RUN REDUCE 3.4 AFTER REDUCE #{display_info} results size: #{results.size}"
      debug "RUN REDUCE 3.4 AFTER REDUCE #{display_info} results:", results
      return results
    end
  
    def run_reduce_partitioner(map_data,num_reducers)
      if not @reduce_partitioner
        Partitioner::recombine_and_split.call(map_data, num_reducers) 
      elsif @reduce_partitioner.class == String
        @reduce_partitioner.constantize.reduce_partitioner(map_data, num_reducers)
      else
        @reduce_partitioner.call(map_data, num_reducers)
      end
    end
    
    ### END CLASS
  end
  
  class AsyncJob < Skynet::Job
           
   ## XXX Partitioning doesn't work yet!!!!!
    
    def initialize(opts = {})
      opts[:async] = true
      super(opts)      
    end


    def map=(klass)
      unless klass.class == String or klass.class == Class
        raise BadMapOrReduceError.new("#{self.class}.map only accepts a class name") 
      end
      klass = klass.to_s if klass.class == Symbol
      @map = klass
    end

    def reduce=(klass)
      unless klass.class == String or klass.class == Class
        raise BadMapOrReduceError.new("#{self.class}.reduce only accepts a class name") 
      end
      klass = klass.to_s if klass.class == Symbol
      @reduce = klass
    end 
    
    # def map_data=(data)
    #   @map_data = Marshal.dump(data)
    # end
    # 
    # def map_data
    #   Marshal.load(@data)
    # end

    def run_master
      if CONFIG[:SOLO]
        run_job
      else
        results = run_tasks(master_task,master_timeout,name)
        self.job_id
      end
    end

    def master_task
      @master_task ||= begin    
        raise Exception.new("No map provided") unless @map
        set_version
        job = Skynet::Job.new
        job.map_timeout = map_timeout
        job.reduce_timeout = reduce_timeout
        job.job_id = task_id
        job.map_data = @map_data
        job.map_name = map_name || name
        job.reduce_name = reduce_name || name
        job.map = @map
        job.map_partitioner = @map_partitioner
        job.reduce = @reduce
        job.reduce_partitioner = @reduce_partitioner
        job.map_tasks = @map_tasks
        job.reduce_tasks = @reduce_tasks
        job.name = @name
        job.version = version
        job.result_timeout = result_timeout
        job.master_result_timeout = master_result_timeout
        
        # pp "job in master task", job
        task = Skynet::Task.new(
          :task_id => task_id, 
          :data => nil, 
          :process => job.to_h, 
          :map_or_reduce => :master,
          :name => self.name,
          :result_timeout => master_result_timeout
        )
      end
    end
    
    def run
      if CONFIG[:solo]
        super
      else
        run_master
      end
    end
        
  end

  # Collection of partitioning utilities
  #
  module Partitioner
  
    # Split one block of data into partitions
    #
    def self.args_pp(*args)
      "#{args.length > 0 ? args.pretty_print_inspect : ''}"
    end

    def self.debug(msg,*args)
      log = Skynet::Logger.get
      log.debug "#{self.class} PARTITION: #{msg} #{args_pp(*args)}"
    end

    def self.simple_partition_data(data, partitions)    
      partitioned_data = Array.new

      # If data size is significantly greater than the number of desired
      # partitions, we can divide the data roughly but the last partition
      # may be smaller than the others.
      #      
      return data if (not data) or data.empty?
      
      if partitions >= data.length
        data.each do |datum|
         partitioned_data << [datum]
        end
      elsif (data.length >= partitions * 2)
        # Use quicker but less "fair" method
        size = data.length / partitions

        if (data.length % partitions != 0)
          size += 1 # Last slice of leftovers
        end

        (0..partitions - 1).each do |i|
          partitioned_data[i] = data[i * size, size]
        end
      else
        # Slower method, but partitions evenly
        partitions = (data.size < partitions ? data.size : partitions)
        (0..partitions - 1).each { |i| partitioned_data[i] = Array.new }
    
        data.each_with_index do |datum, i|
          partitioned_data[i % partitions] << datum
        end
      end
    
      partitioned_data
    end
  
    # Tries to be smart about what kind of data its getting, whether array of arrays or array of arrays of arrays.
    #
    def self.recombine_and_split
      lambda do |map_data, new_partitions|

        return map_data unless map_data.is_a?(Array) and (not map_data.empty?) and map_data.first.is_a?(Array) and (not map_data.first.empty?)
        if not map_data.first.first.is_a?(Array)
          partitioned_data = map_data.flatten
        else
          partitioned_data = map_data.inject(Array.new) do |data,part| 
            data += part
          end    
        end    
        partitioned_data = Partitioner::simple_partition_data(partitioned_data, new_partitions)
        debug "POST PARTITIONED DATA", partitioned_data
        partitioned_data
      end
    end
  
  
    # Smarter partitioner for array data, generates simple sum of array[0]
    # and ensures that all arrays sharing that key go into the same partition.
    #
    def self.array_data_split_by_first_entry
      lambda do |partitioned_data, new_partitions|
        partitions = Array.new
        (0..new_partitions - 1).each { |i| partitions[i] = Array.new }

        partitioned_data.each do |partition|
          partition.each do |array|
            next unless array.class == Array and array.size == 2
            if array[0].kind_of?(Fixnum)
              key = array[0]
            else
              key = 0
              array[0].each_byte { |c| key += c }
            end
            partitions[key % new_partitions] << array
          end
        end

        partitions
      end
    end
  
  end
  
end
