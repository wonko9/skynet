class SkynetWorkerQueue < ActiveRecord::Base
end


class Skynet

  class MessageQueueAdapter
  	
    class Mysql < Skynet::MessageQueueAdapter::Mysql

      def write_worker_status(task, timeout=nil)
        message = Skynet::WorkerStatusMessage.new(task)
        update_hash = message_to_hash(message, timeout, Skynet::WorkerStatusMessage.fields)
        update_hash.each do |k,v|
          if not v
            update_hash[k] = "NULL"
          elsif v.kind_of?(String) or v.kind_of?(Symbol)
            update_hash[k] = "'#{v}'"
          end
        end

        update_sql = "UPDATE skynet_worker_queues SET #{update_hash.collect{|k,v| "#{k}=#{v}"}.join(',')} WHERE worker_id=#{message.worker_id}"
        rows = update(update_sql)
        if rows == 0
          begin
            insert_sql = "INSERT INTO skynet_worker_queues (#{update_hash.keys.join(',')}) VALUES (#{update_hash.values.join(',')})"
            rows = update(insert_sql)
          rescue ActiveRecord::StatementInvalid => e
            if e.message =~ /Duplicate/
              error "DUPLICATE WORKER #{e.message} #{e.backtrace.join("\n")}"
            else
              raise e
            end
          end
        end
        return rows
      end
      
      def take_worker_status(task, timeout=nil)                 
        conditions = template_to_conditions(Skynet::WorkerStatusMessage.worker_status_template(task), Skynet::WorkerStatusMessage.fields)
        worker_status = nil       
        SkynetWorkerQueue.transaction do 
          worker_row = SkynetWorkerQueue.find(:first, :conditions => conditions)
          return unless worker_row
          worker_status = Skynet::WorkerStatusMessage.new(worker_row.clone.attributes)
          worker_row.destroy                                              
        end
        worker_status
      end
      
      def read_all_worker_statuses(hostname=nil,process_id=nil)
        ws = Skynet::WorkerStatusMessage.all_workers_template(hostname)
        ws[4] = process_id if process_id
        conditions = template_to_conditions(ws,Skynet::WorkerStatusMessage.fields)
        rows = SkynetWorkerQueue.find(:all, :conditions => conditions)
        workers = rows.collect{ |w| Skynet::WorkerStatusMessage.new(w.attributes) }#.sort{ |a,b| a.process_id <=> b.process_id }
      end

      def take_all_worker_statuses(*args)
        statuses = []
        read_all_worker_statuses(*args).each do |worker|
          statuses << take_worker_status(worker)
        end        
      end

      def clear_worker_status(hostname=nil)    
        sql = "delete from skynet_worker_queues "
        if hostname                             
          sql << "where hostname = '#{hostname}'"
        end
        SkynetWorkerQueue.connection.execute(sql)        
      end

    end
  end
end
