class Skynet

  # This class is the interface to the Skynet Worker Queue.
  class WorkerQueue < Skynet::MessageQueue

    include SkynetDebugger

    require 'forwardable'
    extend Forwardable

    def self.adapter_class
      "Skynet::WorkerQueueAdapter::TupleSpace"
    end

    def mq
      @mq ||= message_queue_proxy_class.new(:drburi => Skynet::CONFIG[:TS_WQUEUE_URI], :use_ringserver => false)
    end
    
    def_delegators :mq, :take_next_task, :stats, :take_worker_status, :write_worker_status,
                   :take_all_worker_statuses, :read_all_worker_statuses, :clear_worker_status
                   
  end
end
