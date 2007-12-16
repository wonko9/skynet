class Skynet

  class Error < StandardError
  end

  class ConnectionError < Skynet::Error
  end

  class QueueTimeout < Skynet::Error
  end

  class RequestExpiredError < Skynet::Error
  end

  class MessageQueue

    include SkynetDebugger

    require 'forwardable'    
    extend Forwardable    
    
    # require 'skynet_message'                            
    
    def self.adapter
      Object.module_eval(Skynet::CONFIG[:MESSAGE_QUEUE_ADAPTER], __FILE__, __LINE__).adapter
    end

    def initialize(message_queue_proxy_class=Skynet::CONFIG[:MESSAGE_QUEUE_ADAPTER])
      if message_queue_proxy_class.is_a?(String)
        @message_queue_proxy_class = Object.module_eval(message_queue_proxy_class, __FILE__, __LINE__)
      else
        @message_queue_proxy_class = message_queue_proxy_class
      end
      mq
    end

    def message_fields
      Skynet::Message.fields
    end

    def mq
      @mq ||= @message_queue_proxy_class.new
    end
               
    def_delegators :mq, :take_next_task, :write_message, :take_result, :write_error, :write_result,
                   :list_tasks, :list_results,
                   :clear_outstanding_tasks, :clear_outstanding_results,
                   :take_worker_status, :write_worker_status, :read_all_worker_statuses, :clear_worker_status,
                   :get_worker_version, :set_worker_version, :stats
                           

                   

    # def stats
    #   t1 = Time.now
    #   tasks = list_tasks
    #   results = list_results
    #   t2 = Time.now - t1
    #   p_tasks = tasks.partition {|task| task[9] == 0}
    #   {:taken_tasks => p_tasks[1].size, :untaken_tasks => p_tasks[0].size, :results => list_results.size, :time => t2.to_f}
    # end

    def print_stats
      "TAKEN TASKS: #{list_tasks(1).size}, UNTAKEN_TASKS: #{list_tasks(0).size} RESULTS: #{list_results.size}"
    end

    def list
      list_tasks + list_results
    end

		def increment_worker_version
			newver = self.get_worker_version + 1
	    self.set_worker_version(newver)
			newver
		end


    # def write_manager_status(task, timeout=nil)
    #   mq.write(Skynet::ManagerStatusMessage.new(task), timeout)
    # end
    # 
    # def take_manager_status(task, timeout=nil)
    #   template = Skynet::ManagerStatusMessage.new([:status, :manager, MACHINE_NAME])
    #   mq.take(template, timeout)
    # end

    # def read_all_manager_statuses
    #   ms = Skynet::ManagerStatusMessage.new([:status, :manager])
    #   manager_messages = mq.read_all(ms).collect {|ret| Skynet::ManagerStatusMessage.new(ret)}#.sort{ |a,b| a.hostname <=> b.hostname }
    # end


    # def monitor_loop(refresh=3)
    #   loop { ansi_clear; puts monitor_view; sleep refresh}
    # end

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

