class Skynet

  class Error < StandardError
  end

  class ConnectionError < Skynet::Error
  end

  class QueueTimeout < Skynet::Error
  end

  class RequestExpiredError < Skynet::Error
  end

  # This class is the interface to the Skynet Message Queue.
  class MessageQueue

    include SkynetDebugger

    require 'forwardable'
    extend Forwardable

    def self.adapter
      adapter_class.constantize.adapter
    end

    def self.adapter_class
      Skynet::CONFIG[:MESSAGE_QUEUE_ADAPTER]
    end
    
    def self.start_or_connect(adapter_class = self.adapter_class)
      begin
        mq = new
      rescue Skynet::ConnectionError
        if self.adapter == :tuplespace
          pid = fork do
            exec("skynet_tuplespace_server start")
          end
          sleep 5
        end
        new
      end
    end
    
    def initialize(message_queue_proxy_class=Skynet::CONFIG[:MESSAGE_QUEUE_ADAPTER])
      mq
    end

    def self.message_queue_proxy_class
      adapter_class.constantize
    end

    def message_queue_proxy_class
      @message_queue_proxy_class ||= self.class.message_queue_proxy_class
    end

    # Is this version still active in the queue?
    def version_active?(version,queue_id=0)
      mq.version_active?(version,queue_id)
    end

    # Retrieves the current worker version
    def get_worker_version
      mq.get_worker_version
    end

    # Sets the current worker version (causing workers to restart)
    def set_worker_version(version)
      mq.set_worker_version(version)
    end

    # Increments the current worker version (causing workers to restart)
    def increment_worker_version
      newver = self.get_worker_version + 1
      self.set_worker_version(newver)
      newver
    end

    def mq
      @mq ||= message_queue_proxy_class.new
    end

    def_delegators :mq, :take_next_task, :write_message, :take_result, :write_error, :write_result,
                   :list_tasks, :list_results, :clear_outstanding_tasks, :clear_outstanding_results, :stats



   def message_fields
     Skynet::Message.fields
   end

    def print_stats
      "TAKEN TASKS: #{list_tasks(1).size}, UNTAKEN_TASKS: #{list_tasks(0).size} RESULTS: #{list_results.size}"
    end

    def list
      list_tasks + list_results
    end

    def ansi_clear
      puts "\033[2J\033[H"
    end
  end
end

    # def monitor_view
    #   ws = Skynet::WorkerStatusMessage.new([:status, :worker])
    #   ms = Skynet::ManagerStatusMessage.new([:status, :manager])
    #   local_stats = self.stats
    #   stattxt = []
    #   stattxt << "Last Update Time: #{Time.now}\n"
    #   stattxt << '%10s %10s %10s' % ["Untaken", "Taken", "Results"]
    #   stattxt << '%10d %10d %10d' % [local_stats[:untaken_tasks], local_stats[:taken_tasks], local_stats[:results]]
    #   stattxt << '%3s %-20s %10s %20s %20s %5s %5s %10s %s' % ['#', 'hostname', 'process_id', 'job_id', 'task_id', 'ver', 'proc', 'm/r', 'name' ]
    #   messages = mq.read_all(ws).collect{|tuple| Skynet::WorkerStatusMessage.new(tuple)}
    #   messages.sort!{ |a,b| a.process_id <=> b.process_id }
    #   messages.each_with_index do |message,index|
    #     stattxt << '%3d %-20s %10d %20d %20d %5d %5d %10s %s' % [
    #       index,
    #       message.hostname,
    #       message.process_id,
    #       message.job_id,
    #       message.task_id,
    #       message.version,
    #       message.processed,
    #       message.map_or_reduce || '-' ,
    #       message.name
    #     ]
    #   end
    #   return stattxt.join("\n")
    # end

