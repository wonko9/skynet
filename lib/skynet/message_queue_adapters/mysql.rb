class SkynetMessageQueue < ActiveRecord::Base
end

class SkynetWorkerQueue < ActiveRecord::Base
end


class Skynet

  # require 'mysql'
  
  class Error < StandardError
  end
  
  class RequestExpiredError < Skynet::Error
	end
	
	class InvalidMessage < Skynet::Error
  end

  class MessageQueueAdapter
  	
    class Mysql < Skynet::MessageQueueAdapter
      
      include SkynetDebugger
      include Skynet::GuidGenerator

      SEARCH_FIELDS = [:tasktype, :task_id, :job_id, :payload_type, :expire_time, :iteration, :version] unless defined?(SEARCH_FIELDS)
      
      Skynet::CONFIG[:MYSQL_MESSAGE_QUEUE_TEMP_CHECK_DELAY] ||= 30
      
      @@db_set = false
      
      def self.adapter
        :mysql
      end

      def initialize
        if Skynet::CONFIG[:MYSQL_MESSAGE_QUEUE_TABLE]
          SkynetMessageQueue.table_name = Skynet::CONFIG[:MYSQL_MESSAGE_QUEUE_TABLE]
        end
        if Skynet::CONFIG[:MYSQL_QUEUE_DATABASE] and not @@db_set
          begin
            SkynetMessageQueue.establish_connection Skynet::CONFIG[:MYSQL_QUEUE_DATABASE]
            SkynetWorkerQueue.establish_connection Skynet::CONFIG[:MYSQL_QUEUE_DATABASE]
          rescue ActiveRecord::AdapterNotSpecified => e
            warn "#{Skynet::CONFIG[:MYSQL_QUEUE_DATABASE]} not defined as a database adaptor #{e.message}"
          end
        end
        @@db_set = true
        
      end
      
      def message_queue_table
        Skynet::CONFIG[:MYSQL_MESSAGE_QUEUE_TABLE] || SkynetMessageQueue.table_name
      end
      
      def self.debug_class_desc
        "MYSQLMQ"
      end
      
      def message_to_conditions(message)               
        template_to_conditions(message.to_a)
      end

      def template_to_conditions(template,fields=Skynet::Message.fields)
        conditions = []             
        values = []            
        
        fields.each_with_index do |field,ii|
          value = template[ii]
          next unless value
          if value.is_a?(Range)              
            conditions << "#{field} BETWEEN #{value.first} AND #{value.last}"
          elsif value.is_a?(Symbol) or value.is_a?(String)
            conditions << "#{field} = '#{value}'"
          else
            conditions << "#{field} = #{value}"
          end                        
        end
        return '' if conditions.empty?
        return conditions.join(" AND ")
      end
            
      def message_to_hash(message,timeout=nil,fields=Skynet::Message.fields)
        timeout ||= message.expiry        
        hash = {}
        fields.each do |field|
          next if field == :drburi
          # next unless message.send(field)
          if message.send(field).is_a?(Symbol)
            hash[field] = message.send(field).to_s
          elsif field == :payload
            hash[:raw_payload] = message.raw_payload
          else
            hash[field] = message.send(field)
          end
        end
        if timeout
          hash[:timeout] = timeout
          hash[:expire_time] = (Time.now.to_f + timeout) unless hash[:expire_time]
        end
        hash
      end

      # def take(template,start=Time.now, timeout=1, sleep_time=nil, queue_id=0)      
      #   sleep_time ||= timeout
      #   times_tried = 0
      #   payload_type = template[Skynet::Message.fields.index(:payload_type)]
      #   payload_type ||= :any                                             
      #   payload_type = payload_type.to_sym
      # 
      #   10.times do         
      #     begin              
      #       ## TEPERATURE
      #       message = find_next_message(template,payload_type)
      #       if message
      #         rows = set_message_tran_id(message)
      #         if rows < 1
      #           old_temp = temperature(payload_type)
      #           set_temperature(payload_type, template_to_conditions(template), queue_id)
      #           debug "MISSCOLLISION PTYPE #{payload_type} OLDTEMP: #{old_temp} NEWTEMP: #{temperature(payload_type)}"
      #           next 
      #         end
      #         return message
      #       else
      #         old_temp = temperature(payload_type)                                                              
      #         set_temperature(payload_type, template_to_conditions(template), queue_id)
      #         debug "MISS PTYPE #{payload_type} OLDTEMP: #{old_temp} NEWTEMP: #{temperature(payload_type)}"
      #         break if temperature(payload_type) == 1 and old_temp == 1
      #         next
      #       end
      #     rescue ActiveRecord::StatementInvalid => e
      #       if e.message =~ /Deadlock/                                 
      #         old_temp = temperature(payload_type)
      #         set_temperature(payload_type, template_to_conditions(template), queue_id)
      #         debug "COLLISION PTYPE #{payload_type} OLDTEMP: #{old_temp} NEWTEMP: #{temperature(payload_type)}"
      #         next
      #       else
      #         raise e
      #       end
      #     end
      #   end
      # 
      #   if Time.now.to_f > start.to_f + timeout
      #     debug "MISSTIMEOUT PTYPE #{payload_type} #{temperature(payload_type)}"
      #     raise Skynet::RequestExpiredError.new
      #   else    
      #     sleepy = rand(sleep_time * 0.5 )
      #     debug "EMPTY QUEUE #{temperature(payload_type)} SLEEPING: #{sleep_time} / #{sleepy}"
      #     sleep sleepy
      #     return false 
      #   end
      # end

      @@temperature ||= {}
      @@temperature[:task] ||= 1
      @@temperature[:master] ||= 1
      @@temperature[:any] ||= 1
      
      def write_fallback_message(message_row, message)
        tran_id  = get_unique_id(1)
        ftm = message.fallback_task_message            
        update_sql = %{
          update #{message_queue_table} 
          SET iteration = #{ftm.iteration }, 
          expire_time = #{ftm.expire_time}, 
          updated_on = '#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}',
          tran_id = #{tran_id}                   
          WHERE id = #{message_row.id} AND iteration = #{message.iteration}
          AND tran_id #{(message_row.tran_id ? " =#{message_row.tran_id}" : ' IS NULL')}
        }            
        rows = update(update_sql) || 0
        message_row.tran_id = tran_id if rows == 1
        rows        
      end

      def take_next_task(curver,timeout=0.5,payload_type=nil,queue_id=0)
        timeout = Skynet::CONFIG[:MYSQL_NEXT_TASK_TIMEOUT] if timeout < 1
        debug "TASK NEXT TASK!!!!!!! timeout: #{timeout} queue_id:#{queue_id}"     

        start    = Time.now    
        template = Skynet::Message.next_task_template(curver, payload_type)        
        message  = nil
        
        loop do
          rows     = 0
          message  = nil
          template = Skynet::Message.next_task_template(curver, payload_type)
          begin              
            message_row = find_next_message(template, payload_type)
            if message_row
              message = Skynet::Message.new(message_row.attributes)
              rows = write_fallback_message(message_row, message)

              if rows < 1
                old_temp = temperature(payload_type)
                set_temperature(payload_type, template_to_conditions(template), queue_id)
                debug "MISSCOLLISION PTYPE #{payload_type} OLDTEMP: #{old_temp} NEWTEMP: #{temperature(payload_type)}"
              else
                break
              end
            else # no messages on queue with this temp
              old_temp = temperature(payload_type)                                                              
              if old_temp > 1
                set_temperature(payload_type, template_to_conditions(template), queue_id)
              end
              debug "MISS PTYPE #{payload_type} OLDTEMP: #{old_temp} NEWTEMP: #{temperature(payload_type)}"
            end
          rescue Skynet::Message::BadMessage => e
            message_row.destroy
            next
          rescue ActiveRecord::StatementInvalid => e
            if e.message =~ /Deadlock/                                 
              old_temp = temperature(payload_type)
              set_temperature(payload_type, template_to_conditions(template), queue_id)
              debug "COLLISION PTYPE #{payload_type} OLDTEMP: #{old_temp} NEWTEMP: #{temperature(payload_type)}"
            else
              raise e
            end
          end
          if Time.now.to_f > start.to_f + timeout
            debug "MISSTIMEOUT PTYPE #{payload_type} #{temperature(payload_type)}"
            raise Skynet::RequestExpiredError.new
          else    
            sleepy = rand(timeout * 0.5 )
            debug "EMPTY QUEUE #{temperature(payload_type)} SLEEPING: #{timeout} / #{sleepy}"
            sleep sleepy
          end
        end

        return message
      end
      
      def write_message(message,timeout=nil)
        timeout ||= message.expiry
        SkynetMessageQueue.create(message_to_hash(message, timeout))
      end
      
      def write_result(message,result=[],timeout=nil)
        timeout ||= message.expiry
        result_message = message.result_message(result)
        result_message.expire_time = nil
        update_message_with_result(result_message,timeout)
      end
      
      def update_message_with_result(message,timeout=nil)
        timeout ||= message.expiry
        timeout_sql = (timeout ? ", timeout = #{timeout}, expire_time = #{Time.now.to_f + timeout}" : '')
        rows = 0      
        raw_payload_sql = " raw_payload = "
        raw_payload_sql << (message.raw_payload ? "'#{::Mysql.escape_string(message.raw_payload)}'" : 'NULL')
        update_sql = %{
          update #{message_queue_table} 
          set tasktype = "#{message.tasktype}", 
          #{raw_payload_sql},
          payload_type = "#{message.payload_type}",
          updated_on = "#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}",
          tran_id = NULL
          #{timeout_sql}
          where task_id = #{message.task_id}
          }                     
        rows = update(update_sql)                  
         
        raise Skynet::RequestExpiredError.new() if rows == 0         
      end

      def take_result(job_id,timeout=1)
        start = Time.now
        result = nil                
        sleep_time = 10
        if timeout < 1
          sleep_time = 1
        elsif timeout > 10
          sleep_time = 10
        else
          sleep_time = timeout * 0.25
        end
        message_row = nil
        
        loop do
          # message_row = take(Skynet::Message.result_template(job_id), start, timeout,sleep_time)
          conditions = template_to_conditions(Skynet::Message.result_template(job_id))
          # sleep_time ||= timeout

          message_row = SkynetMessageQueue.find(:first,:conditions => conditions)
          
          break if message_row

          if Time.now.to_f > start.to_f + timeout
            raise Skynet::RequestExpiredError.new
          else          
            sleepy = rand(sleep_time)
            # error "RESULT EMPTY SLEEPING: #{sleepy}"          
            sleep sleepy
            next
          end          
          next
        end                      

        result = Skynet::Message.new(message_row.clone.attributes)
        message_row.destroy
        return result if result        
      end
      
      def list_tasks(iteration=nil)
        conditions = template_to_conditions(Skynet::Message.outstanding_tasks_template(iteration))
        SkynetMessageQueue.find(:all,:conditions => conditions)
      end
      
      def list_results
        conditions = template_to_conditions(Skynet::Message.outstanding_results_template)
        SkynetMessageQueue.find(:all,:conditions => conditions)
      end

      def write_error(message,error='',timeout=nil)
        message.expire_time = nil
        update_message_with_result(message.error_message(error),timeout)
      end

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


      def clear_worker_status(hostname=nil)    
        sql = "delete from skynet_worker_queues "
        if hostname                             
          sql << "where hostname = '#{hostname}'"
        end
        SkynetWorkerQueue.connection.execute(sql)        
      end

      def set_worker_version(ver=nil)                      
        ver ||= 1
        SkynetMessageQueue.connection.insert("replace #{message_queue_table} (tran_id, task_id, tasktype, version) values (0, 0, 'version',#{ver})")
        ver
      end

      def get_worker_version
        ver = SkynetMessageQueue.connection.select_value("select version from #{message_queue_table} where tran_id = 0 and tasktype = 'version'")
        if not ver
          begin
            SkynetMessageQueue.connection.insert("insert into #{message_queue_table} (tran_id, task_id, tasktype, version) values (0, 0, 'version', 1)")
          rescue ActiveRecord::StatementInvalid => e
            if e.message =~ /Duplicate/
              return get_worker_version
            else
              raise e
            end
          end
          ver = 1
        end
        ver.to_i
      end
      
      def clear_outstanding_tasks
        SkynetMessageQueue.connection.execute("delete from #{message_queue_table} where tasktype = 'task'")
      end       

      def delete_expired_messages
        SkynetMessageQueue.connection.execute("delete from #{message_queue_table} where (expire_time BETWEEN 1 AND '#{Time.now.to_f}') and iteration = -1")
      end

# select hostname, iteration, count(id) as number_of_workers, count(iteration) as iteration, sum(processed) as processed, max(started_at) as most_recent_task_time from skynet_worker_queues where tasksubtype = 'worker' group by hostname, iteration;      
# 
# select hostname, count(id) as number_of_workers, sum(processed) as processed, max(started_at) as most_recent_task_time,
# CASE iteration WHEN NULL 
# 
# from skynet_worker_queues where tasksubtype = 'worker' group by hostname;      

      def stats
        stats = {
          :servers              => {}, 
          :results              => 0, 
          :taken_tasks          => 0, 
          :untaken_tasks        => 0,
          :taken_master_tasks   => 0,
          :taken_task_tasks     => 0, 
          :untaken_master_tasks => 0,
          :untaken_task_tasks   => 0, 
          :failed_tasks         => 0,
          :processed            => 0, 
          :number_of_workers    => 0,
          :active_workers       => 0,
          :idle_workers         => 0,
          :hosts                => 0,
          :masters              => 0,
          :taskworkers          => 0,
          :time                 => Time.now.to_f
         }

        stat_rows = SkynetWorkerQueue.connection.select_all(%{
          SELECT tasktype, payload_type, iteration, count(id) as number_of_tasks
          FROM #{message_queue_table}
          GROUP BY tasktype, payload_type, iteration          
         })       
         # pp stat_rows
         stat_rows.each do |row|
           if row["tasktype"] == "result" or row["payload_type"] == "result"
             stats[:results] += row["number_of_tasks"].to_i
           elsif row["tasktype"] == "task"    
             type_of_tasks = nil
             if row["payload_type"] == "master"
               type_of_tasks = :master_tasks
             elsif row["payload_type"] == "task"
               type_of_tasks = :task_tasks
             end
             if row["iteration"].to_i > 0
               stats["taken_#{type_of_tasks}".to_sym] += row["number_of_tasks"].to_i 
               stats[:taken_tasks] += row["number_of_tasks"].to_i
             elsif row["iteration"].to_i == 0
               stats["untaken_#{type_of_tasks}".to_sym] += row["number_of_tasks"].to_i 
               stats[:untaken_tasks] += row["number_of_tasks"].to_i
             else
               stats[:failed_tasks] += row["number_of_tasks"].to_i 
             end
           end
         end
        
        servers = {}
        
        stat_sql = <<-SQL
          select hostname, map_or_reduce, count(id) number_of_workers, sum(processed) as processed,
          max(started_at) as most_recent_task_time, iteration 
          FROM skynet_worker_queues
          WHERE skynet_worker_queues.tasksubtype = 'worker'                
        SQL
        
        stat_rows = SkynetWorkerQueue.connection.select_all("#{stat_sql} GROUP BY hostname, map_or_reduce").each do |row|
          servers[row["hostname"]] ||= {            
            :processed             => 0,
            :hostname              => row["hostname"],
            :number_of_workers     => 0,
            :active_workers        => 0,
            :idle_workers          => 0,
          }

          servers[row["hostname"]][:processed]          += row["processed"].to_i
          servers[row["hostname"]][:number_of_workers]  += row["number_of_workers"].to_i
          servers[row["hostname"]][:active_workers]     += 0
          servers[row["hostname"]][:idle_workers]       += row["number_of_workers"].to_i
          stats[:processed]                             += row["processed"].to_i
          stats[:number_of_workers]                     += row["number_of_workers"].to_i
          stats[:idle_workers]                          += row["number_of_workers"].to_i
        end   

        SkynetWorkerQueue.connection.select_all(%{
          #{stat_sql} AND skynet_worker_queues.iteration IS NOT NULL
          GROUP BY hostname, map_or_reduce
        }).each do |row|
          map_or_reduce = nil
          if row["map_or_reduce"] == "master"
            map_or_reduce = :masters
          else
            map_or_reduce = :taskworkers
          end
          servers[row["hostname"]][:active_workers] += row["number_of_workers"].to_i
          servers[row["hostname"]][:idle_workers]   -= row["number_of_workers"].to_i
          servers[row["hostname"]][map_or_reduce]   ||= 0
          servers[row["hostname"]][map_or_reduce]   += row["number_of_workers"].to_i
          stats[map_or_reduce]                      += row["number_of_workers"].to_i
          stats[:active_workers]                    += row["number_of_workers"].to_i
          stats[:idle_workers]                      -= row["number_of_workers"].to_i
        end                                                                                                   
        
        stats[:servers] = servers
        stats[:hosts]   = servers.keys.size
        stats[:time]    = Time.now.to_f - stats[:time]
        stats
      end

      def processed(sleepy=5,tim=10)
        last_time = Time.now
        last_count = Skynet::MessageQueue.new.stats[:processed]
        tim.times do
          new_count = Skynet::MessageQueue.new.stats[:processed]
          new_time = Time.now
          puts "Processed #{new_count - last_count} in #{new_time - last_time}"
          last_time = new_time
          last_count = new_count
          sleep sleepy
        end
      end
      
    private

      def update(sql)                                            
        rows = 0
        3.times do
          begin    
            SkynetMessageQueue.transaction do
              rows = SkynetMessageQueue.connection.update(sql)
            end
            return rows
          rescue ActiveRecord::StatementInvalid => e
            if e.message =~ /Deadlock/ or e.message =~ /Transaction/
              error "#{self.class} update had collision #{e.message}"            
              sleep 0.2
              next
            else
              raise e
            end
          end
        end
        return rows
      end
    
      Skynet::CONFIG[:MYSQL_TEMPERATURE_CHANGE_SLEEP] ||= 40
      
      def find_next_message(template, payload_type)
        conditions = template_to_conditions(template)
        temperature_sql = (temperature(payload_type) > 1 ? " AND id % #{temperature(payload_type).ceil} = #{rand(temperature(payload_type)).to_i} " : '')

        ###  Mqke sure we get the old ones.  If we order by on ever select its VERY expensive.
        order_by = (payload_type != :master and rand(100) < 5) ? "ORDER BY payload_type desc, created_on desc" : '' 
        
        sql = <<-SQL 
          SELECT *
          FROM #{message_queue_table} 
          WHERE #{conditions} #{temperature_sql}
          #{order_by}
          LIMIT 1
        SQL
        
        SkynetMessageQueue.find_by_sql(sql).first
      end

      # def set_message_tran_id(message)
      #   tran_id = get_unique_id(1)
      #   update_sql = %{
      #     UPDATE #{message_queue_table} 
      #     set tran_id = #{tran_id} 
      #     WHERE id = #{message.id}}
      #   update_sql << " AND tran_id " << (message.tran_id ? " =#{message.tran_id}" : ' IS NULL')
      #   pp update_sql
      # 
      #   rows = 0
      #   SkynetMessageQueue.transaction do
      #     rows = SkynetMessageQueue.connection.update(update_sql)
      #   end
      #   message.tran_id = tran_id
      #   rows
      # end
        

      # Skynet::CONFIG[:temperature_growth_rate] ||= 2
      # Skynet::CONFIG[:temperature_backoff_rate] ||= 0.75
      
      # TUNEABLE_SETTINGS = [:temp_pow, :temp_interval, :sleep_time]
      # 
      # def write_score(new_values,new_result,score)
      #   values ||= {}       
      #   set = new_values.keys.sort.collect{|k|[k,new_values[k]]}.join(",")
      #   if not values[set]
      #     values[set] ||= {}
      #     values[set][:results] ||= []
      #     values[set][:scores] ||= []
      #     values[set][:settings] ||= {}
      #     values[set][:total_score] = 0
      #     TUNEABLE_SETTINGS.each do |setting|
      #       values[set][:settings][setting] = []
      #     end
      #   end
      #   TUNEABLE_SETTINGS.each do |setting, value|
      #     values[set][:settings][setting] << value
      #   end         
      #   values[set][:results] << new_result
      #   values[set][:scores] << score + values[set][:total_score]
      #   values[set][:total_score] += score
      # end
        
      def temperature(payload_type)
        payload_type ||= :any                                             
        payload_type   = payload_type.to_sym
        @@temperature[payload_type.to_sym]
      end
      
      Skynet::CONFIG[:MYSQL_QUEUE_TEMP_POW] ||= 0.6
               
      def set_temperature(payload_type, conditions, queue_id=0)                                            
        payload_type ||= :any                                             
        payload_type   = payload_type.to_sym
        
        temp_q_conditions = "queue_id = #{queue_id} AND type = '#{payload_type}' AND updated_on < '#{(Time.now - 5).strftime('%Y-%m-%d %H:%M:%S')}'"
#        "POW(#{(rand(40) + 40) * 0.01})"
# its almost like the temperature table needs to store the POW and adjust that to be adaptive.  Like some % of the time it
# uses the one in the table, and some % it tries a new one and scores it.
        begin
          temperature = SkynetMessageQueue.connection.select_value(%{select (
            CASE WHEN (@t:=FLOOR(
            POW(@c:=(SELECT count(*) FROM #{message_queue_table} WHERE #{conditions}
          ),#{Skynet::CONFIG[:MYSQL_QUEUE_TEMP_POW]}))) < 1 THEN 1 ELSE @t END) from skynet_queue_temperature WHERE #{temp_q_conditions} 
          })
          if temperature
            rows = update("UPDATE skynet_queue_temperature SET temperature = #{temperature} WHERE #{temp_q_conditions}")
            @@temperature[payload_type.to_sym] = temperature.to_f
          else
            sleepy = rand Skynet::CONFIG[:MYSQL_TEMPERATURE_CHANGE_SLEEP]
            sleep sleepy          
            @@temperature[payload_type.to_sym] = get_temperature(payload_type, queue_id)
          end
        rescue ActiveRecord::StatementInvalid => e
          if e.message =~ /away/
            ActiveRecord::Base.connection.reconnect!
            SkynetMessageQueue.connection.reconnect!
          end
        end
        # update("UPDATE skynet_queue_temperature SET type = '#{payload_type}', temperature = CASE WHEN @t:=FLOOR(SQRT(select count(*) from #{message_queue_table} WHERE #{conditions})) < 1 THEN 1 ELSE @t END")
        # tasks = SkynetMessageQueue.connection.select_value("select count(*) from #{message_queue_table} WHERE #{conditions}").to_i
        # sleep 4 if payload_type == :tasks and tasks < 100
        # @@temperature[payload_type.to_sym] = tasks ** 0.5
        # @@temperature[payload_type.to_sym] *= multiplier
        @@temperature[payload_type.to_sym] = 1 if @@temperature[payload_type.to_sym] < 1
      end
      
      def get_temperature(payload_type, queue_id=0)
        payload_type ||= :any                                             
        payload_type   = payload_type.to_sym
        value = SkynetMessageQueue.connection.select_value("select temperature from skynet_queue_temperature WHERE type = '#{payload_type}'").to_f
        if not value
          SkynetMessageQueue.connection.execute("insert into skynet_queue_temperature (queue_id,type,temperature) values (#{queue_id},'#{payload_type}',#{@@temperature[payload_type.to_sym]})")
        end
        value
      end
    end
  end
end
