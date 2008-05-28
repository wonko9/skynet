require 'rinda/tuplespace'
class Rinda::TupleSpaceProxy
  def take(tuple, sec=nil, &block)
    port = []
    port.push @ts.move(nil, tuple, sec, &block)
    port[0]
  end
end

class Skynet
  class Error < StandardError
  end

  class RequestExpiredError < Skynet::Error
	end

	class InvalidMessage < Skynet::Error
  end

  class MessageQueueAdapter

    class TupleSpace < Skynet::MessageQueueAdapter

      include SkynetDebugger

      USE_FALLBACK_TASKS = false

      @@ts = nil
      @@curhostidx = 0

      def self.debug_class_desc
        "TUPLESPACE"
      end

      def self.adapter
        :tuplespace
      end

      def self.start_or_connect(options={})
        begin
          mq = new
        rescue Skynet::ConnectionError
          pid = fork do
            exec("skynet_tuplespace_server start")
          end
          sleep 5
          mq = new
        end
      end

      attr_accessor :start_options
      
      def initialize(options={})
        @start_options = options
        @ts = self.class.get_tuple_space(options)
      end

      def dequeue_task(version,tasktype,timeout=0.00001,queue_id=0)
        real_tasktype = tasktype.to_sym
        if tasktype.to_s == :master_or_task
          real_tasktype = [:master,:task].rand
        end
        template = {                           
          ### FIXME expire_time should just be a number
          :expire_time  => (0 .. Time.now.to_f),
          :tasktype     => real_tasktype,
          :queue_id     => queue_id,
          :version      => version,
          :iteration    => (0..Skynet::CONFIG[:MAX_RETRIES]),
        }
        ts_template = Skynet::TaskMessage.fields.collect do |field|
          if template[field]
            template[field]
          else
            nil
          end
        end
        ts_template.unshift(:task)
                                          
        tuple = take(ts_template,timeout)
        tuple.shift
        message = Skynet::TaskMessage.new(tuple)
        message
      end
      
      def enqueue_task(message)
        timeout = message.result_timeout if message.result_timeout and message.result_timeout > 0
        timeout ||= 10
        message.tasktype = message.tasktype.to_s
        message_array = message.to_a
        message_array[Skynet::TaskMessage.fields.index(:expire_time)] ||= 0
        message_array.unshift(:task)
        write(message_array,timeout)
      end

      def enqueue_result(message)
        # First we'll make sure that task is acknowledged so it is taken off the Q permanently
        acknowledge(message)
        result_message = message.result_message(result).to_a
        result_message.unshift(:result)
        timeout = result_message.timeout
        write(result_message,timeout)
      end

      # def write_result(message,result=[],timeout=nil)
      #   result_message = message.result_message(result).to_a
      #   timeout ||= result_message.timeout
      #   write(result_message,timeout)
      #   take_fallback_message(message)
      #   result_message
      # end

      def dequeue_result(job_id,timeout=1)
        template_message = Skynet::ResultMessage.new(:job_id => job_id)
        template_message.tasktype = nil
        result_template = template_message.to_a
        result_template.unshift(:result)        
        tuple = Skynet::Message.new(take(result_template,timeout))
        tuple.shift
        tuple
      end

      def accept_unacknowledged_message(message)
        write_failover(message)
      end
      
      def acknowledge(message)    
        take_fallback_message(message,timeout=0.01)
      end

      #subclass
      def unacknowledge(message)
        ## How do we unacknowledge
        take_fallback_message(message,timeout=0.01)
      end










      def list_tasks(iteration=nil,queue_id=0)
        read_all(Skynet::Message.outstanding_tasks_template(iteration,queue_id))
      end

      def list_results
        read_all(Skynet::Message.outstanding_results_template)
      end

      def version_active?(curver=nil, queue_id= 0)
        return true unless curver
        begin
          message_row = read(Skynet::Message.next_task_template(curver, nil, queue_id),0.00001)
          true
        rescue Skynet::RequestExpiredError
          return true if curver.to_i == get_worker_version.to_i
          false
        end
      end

      def get_worker_version
        1
        begin
          message = read([:workerversion,nil],0.00001)
          if message
            curver = message[1]
          else
            curver=0
          end
        rescue Skynet::RequestExpiredError => e
          curver = 0
        end
        curver
      end

      def set_worker_version(ver=nil)
        return true
        begin
          tuples = read_all([:workerversion,nil])
          curver = 0
          tuples.each do |tuple|
            curver = tuple[1]
            debug "CURRENT WORKER VERSION #{curver}"
            curvmessage = take([:workerversion,nil],0.00001)
            if curvmessage
              curver = curvmessage[1]
            else
              curver=0
            end
          end
        rescue Skynet::RequestExpiredError => e
          curver = 0
        end

        newver = ver ? ver : curver + 1
        debug "WRITING CURRENT WORKER REV #{newver}"
        write([:workerversion,never])
        newver
      end

      def flush
        clear_outstanding_tasks
      end
      
      def clear_outstanding_tasks
        task_template = []
        Skynet::ResultMessage.fields.each_with_index do
          task_template << nil
        end
        task_template.unshift(:task)
        begin
          tasks = read_all(task_template)
        rescue DRb::DRbConnError, Errno::ECONNREFUSED => e
          error "ERROR #{e.inspect}", caller
        end

        tasks.size.times do |ii|
          take(task_template,0.00001)
        end
        
        result_template = []
        Skynet::ResultMessage.fields.each_with_index do
          result_template << nil
        end
        result_template.unshift(:result)
        results = read_all(result_template)
        results.size.times do |ii|
          take(result_template,0.00001)
        end
      end

      def stats
        t1 = Time.now
        tasks = list_tasks
        results = list_results
        t2 = Time.now - t1
        p_tasks = tasks.partition {|task| task[9] == 0}
        {:taken_tasks => p_tasks[1].size, :untaken_tasks => p_tasks[0].size, :results => list_results.size, :time => t2.to_f}
      end

      private

      attr_accessor :ts

      def write(tuple,timeout=nil)
        ts_command(:write,tuple,timeout)
      end

      def take(template,timeout=nil)
        ts_command(:take,template,timeout)
      end

      def read(template,timeout=nil)
        ts_command(:read,template,timeout)
      end

      def read_all(template)
        ts_command(:read_all,template)
      end

      ###### FALLBACK METHODS
      # def write_fallback_task(message)
      #   return unless USE_FALLBACK_TASKS
      #   debug "4 WRITING BACKUP TASK #{message.task_id}", message.to_h
      #   ftm = message.fallback_task_message
      #   debug "WRITE FALLBACK TASK", ftm.to_h
      #   timeout = message.timeout * 8
      #   write(ftm,timeout) unless ftm.iteration == -1
      #   ftm
      # end

      def take_fallback_message(message,timeout=0.01)
        return unless USE_FALLBACK_TASKS
        return if message.retry <= message.iteration
        begin
          fb_message = Skynet::TaskMessage.new(:task_id => message.task_id)
          fb_template = fb_message.to_a
          fb_template.unshift(:task)         
          debug "LOOKING FOR FALLBACK TEMPLATE", fb_template
          take(fb_template,timeout)
          debug "TOOK FALLBACK MESSAGE for TASKID: #{fb_message.task_id}"
        rescue Skynet::RequestExpiredError => e
          error "Couldn't find expected FALLBACK MESSAGE", fb_template
        end
      end
      ## END FALLBACK METHODS

      def ts_command(command,message,timeout=nil)
        # tries = 0
        # until(tries > 3)
        if message.is_a?(Skynet::Message)
          tuple = message.to_a
        elsif message.is_a?(Array)
          tuple = message
        else
          raise InvalidMessage.new("You must provide a valid Skynet::Message object when calling #{command}.  You passed #{message.inspect}.")
        end

        begin
          if command==:read_all
            return ts.send(command,tuple)
          else
            return ts.send(command,tuple,timeout)
          end

        rescue Rinda::RequestExpiredError
          raise Skynet::RequestExpiredError.new
        rescue DRb::DRbConnError => e
          begin
            error "Couldnt run command [#{command}] on tuplespace. start options: #{@start_options.inspect}"
            @ts = self.class.get_tuple_space(@start_options)
            raise Skynet::ConnectionError.new("Can't find ring finger. #{e.inspect} #{@start_options.inspect}")
            # tries += 1
            # next
          rescue Skynet::ConnectionError => e
            raise Skynet::ConnectionError.new("Can't find ring finger. #{e.inspect} #{@start_options.inspect}")
          # rescue RuntimeError => e
          #   raise Skynet::ConnectionError.new("Can't find ring finger. #{}")
          rescue DRb::DRbConnError, Errno::ECONNREFUSED => e
            raise Skynet::ConnectionError.new("There was a problem conected to the #{self.class} #{e.class} #{e.message}")
          end
        end
        # end
      end

  ####################################
  ######## CLASS METHODS #############
  ####################################

      ### XXX ACCEPT MULTIPLE TUPLE SPACES and a flag whether to use replication or failover.

      def self.get_tuple_space(options = {})    
        use_ringserver   = options[:use_ringserver]
        ringserver_hosts = options[:ringserver_hosts]
        drburi           = options[:drburi]
        
        return @@ts if valid_tuplespace?(@@ts)
        loop do
          begin
            DRb.start_service
            if use_ringserver
              ringserver_hosts[@@curhostidx] =~ /(.+):(\d+)/
              host = $1
              port = $2.to_i
              @@ts = connect_to_tuple_space(host,port,use_ringserver)
            else
              drburi = "druby://#{drburi}" unless drburi =~ %r{druby://}
              @@ts = get_tuple_space_from_drburi(drburi)
              raise DRb::DRbConnError.new unless valid_tuplespace?(@@ts)
              info "#{self} CONNECTED TO #{drburi}"
            end
            return @@ts
          rescue RuntimeError => e
            if ringserver_hosts[@@curhostidx + 1]
              error "#{self} Couldn't connect to #{ringserver_hosts[@@curhostidx]} trying #{ringserver_hosts[@@curhostidx+1]}"
              @@curhostidx += 1
              next
            else
              raise Skynet::ConnectionError.new("Can't find ring finger @ #{ringserver_hosts[@@curhostidx]}. #{e.class} #{e.message}")
            end
          rescue Exception => e
            raise Skynet::ConnectionError.new("Error getting tuplespace @ #{ringserver_hosts[@@curhostidx]}. #{e.class} #{e.message}")
          end
        end
        return @@ts
      end

      def self.connect_to_tuple_space(host,port,use_ringserver=Skynet::CONFIG[:TS_USE_RINGSERVER])
        info "#{self} trying to connect to #{host}:#{port}"
        if use_ringserver
          ring_finger = Rinda::RingFinger.new(host,port)
          ring_server = ring_finger.lookup_ring_any(0.5)

          ringts = ring_server.read([:name, :TupleSpace, nil, nil],0.00005)[2]
          ts = Rinda::TupleSpaceProxy.new(ringts)
        else
          ts = get_tuple_space_from_drburi("druby://#{host}:#{port}")
        end
        info "#{self} CONNECTED TO #{host}:#{port}"
        ts
      end

      def self.get_tuple_space_from_drburi(drburi)
        DRbObject.new(nil, drburi)
      end

      def self.valid_tuplespace?(ts)
        return false unless ts
        begin
          ts.read_all([:valid])
          return true
        rescue DRb::DRbConnError, RuntimeError, Errno::ECONNREFUSED  => e
          return false
        end
      end
    end
  end
end
