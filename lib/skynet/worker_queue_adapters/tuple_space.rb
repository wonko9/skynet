class Skynet
  
  class WorkerQueueAdapter
  	
    class TupleSpace < Skynet::MessageQueueAdapter::TupleSpace

      def write_worker_status(task, timeout=nil)
        begin
          take_worker_status(task,0.00001)
        rescue Skynet::RequestExpiredError
        end   
        write(Skynet::WorkerStatusMessage.new(task), timeout)
      end

      def take_worker_status(task, timeout=nil)
        Skynet::WorkerStatusMessage.new(take(Skynet::WorkerStatusMessage.worker_status_template(task), timeout))
      end

      def read_all_worker_statuses(hostname=nil)
        ws = Skynet::WorkerStatusMessage.all_workers_template(hostname)
        workers = read_all(ws).collect{ |w| Skynet::WorkerStatusMessage.new(w) }#.sort{ |a,b| a.process_id <=> b.process_id }
      end

      def clear_worker_status(hostname=nil)
        cnt = 0
        begin
          loop do
            take(Skynet::WorkerStatusMessage.new([:status, :worker, hostname, nil, nil]),0.01)
            cnt += 1
          end
        rescue Skynet::RequestExpiredError
        end
        cnt
      end
    end
  end
end
