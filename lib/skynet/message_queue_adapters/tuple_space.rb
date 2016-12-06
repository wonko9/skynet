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

      USE_FALLBACK_TASKS = true

      @@ts = nil
      @@curhostidx = 0

      def self.debug_class_desc
        "TUPLESPACE"
      end

      def self.adapter
        :tuplespace
      end

      attr_accessor :start_options

      def initialize(options={})
        @start_options = options
        @ts = self.class.get_tuple_space(options)
      end

      def take_next_task(curver,timeout=nil,payload_type=nil,queue_id=0)
        message = Skynet::Message.new(take(Skynet::Message.next_task_template(curver,payload_type, queue_id),timeout))
        write_fallback_task(message)
        message
      end

      def write_message(message,timeout=nil)
        timeout ||= message.expiry
        write(message,timeout)
      end

      def write_result(message,result=[],timeout=nil)
        result_message = message.result_message(result).to_a
        timeout ||= result_message.expiry
        write(result_message,timeout)
        take_fallback_message(message)
        result_message
      end

      def take_result(job_id,timeout=nil)
        Skynet::Message.new(take(Skynet::Message.result_template(job_id),timeout))
      end

      def write_error(message,error='',timeout=nil)
        timeout ||= message.expiry
        write(message.error_message(error),timeout)
        take_fallback_message(message)
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
        begin
          message = Skynet::WorkerVersionMessage.new(read(Skynet::WorkerVersionMessage.template,0.00001))
          if message
            curver = message.version
          else
            curver=0
          end
        rescue Skynet::RequestExpiredError => e
          curver = 0
        end
        curver
      end

      def set_worker_version(ver=nil)
        begin
          messages = read_all(Skynet::WorkerVersionMessage.template).collect {|ret| Skynet::WorkerVersionMessage.new(ret)}
          curver = 0
          messages.each do |message|
            curver = message.version
            debug "CURRENT WORKER VERSION #{curver}"
            curvmessage = Skynet::WorkerVersionMessage.new(take(message.template,0.00001))
            if curvmessage
              curver = curvmessage.version
            else
              curver=0
            end
          end
        rescue Skynet::RequestExpiredError => e
          curver = 0
        end

        newver = ver ? ver : curver + 1
        debug "WRITING CURRENT WORKER REV #{newver}"
        write(Skynet::WorkerVersionMessage.new(:version=>newver))
        newver
      end

      def clear_outstanding_tasks
        begin
          tasks = read_all(Skynet::Message.outstanding_tasks_template)
        rescue DRb::DRbConnError, Errno::ECONNREFUSED => e
          error "ERROR #{e.inspect}", caller
        end

        tasks.size.times do |ii|
          take(Skynet::Message.outstanding_tasks_template,0.00001)
        end

        results = read_all(Skynet::Message.outstanding_results_template)
        results.size.times do |ii|
          take(Skynet::Message.outstanding_results_template,0.00001)
        end

        task_tuples = read_all(Skynet::Message.outstanding_tasks_template)
        result_tuples = read_all(Skynet::Message.outstanding_results_template)
        return task_tuples + result_tuples
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
      def write_fallback_task(message)
        return unless USE_FALLBACK_TASKS
        debug "4 WRITING BACKUP TASK #{message.task_id}", message.to_h
        ftm = message.fallback_task_message
        debug "WRITE FALLBACK TASK", ftm.to_h
        timeout = message.expiry * 8
        write(ftm,timeout) unless ftm.iteration == -1
        ftm
      end

      def take_fallback_message(message,timeout=0.01)
        return unless USE_FALLBACK_TASKS
        return if message.retry <= message.iteration
        begin
          # debug "LOOKING FOR FALLBACK TEMPLATE", message.fallback_template
          fb_message = Skynet::Message.new(take(message.fallback_template,timeout))
          # debug "TOOK FALLBACK MESSAGE for TASKID: #{fb_message.task_id}"
        rescue Skynet::RequestExpiredError => e
          error "Couldn't find expected FALLBACK MESSAGE", Skynet::Message.new(message.fallback_template).to_h
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
