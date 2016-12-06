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

  class MessageQueueAdapter::Mysql < Skynet::MessageQueueAdapter

    include SkynetDebugger
    include Skynet::GuidGenerator

    SEARCH_FIELDS = [:tasktype, :task_id, :job_id, :payload_type, :expire_time, :iteration, :version] unless defined?(SEARCH_FIELDS)

    Skynet::CONFIG[:MYSQL_MESSAGE_QUEUE_TEMP_CHECK_DELAY] ||= 30

    @@db_set = false

    def self.adapter
      :mysql
    end

    def self.start_or_connect(options={})
      new
    end

    def initialize(options={})
      if Skynet::CONFIG[:MYSQL_MESSAGE_QUEUE_TABLE]
        SkynetMessageQueue.table_name = Skynet::CONFIG[:MYSQL_MESSAGE_QUEUE_TABLE]
      end
      if not @@db_set
        if Skynet::CONFIG[:MYSQL_QUEUE_DATABASE]
          begin
            SkynetMessageQueue.establish_connection Skynet::CONFIG[:MYSQL_QUEUE_DATABASE]
            SkynetWorkerQueue.establish_connection Skynet::CONFIG[:MYSQL_QUEUE_DATABASE]
          rescue ActiveRecord::AdapterNotSpecified => e
            error "#{Skynet::CONFIG[:MYSQL_QUEUE_DATABASE]} not defined as a database adaptor #{e.message}"
          end
        elsif (not ActiveRecord::Base.connected?) and Skynet::CONFIG[:MYSQL_DATABASE]
            db_options = {
              :adapter  => Skynet::CONFIG[:MYSQL_ADAPTER],
              :host     => Skynet::CONFIG[:MYSQL_HOST],
              :username => Skynet::CONFIG[:MYSQL_USERNAME],
              :password => Skynet::CONFIG[:MYSQL_PASSWORD],
              :database => Skynet::CONFIG[:MYSQL_DATABASE]
            }
            ActiveRecord::Base.establish_connection(db_options)
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

    def version_active?(curver=nil, queue_id=0)
      return true unless curver
      message_row = find_next_message(Skynet::Message.next_task_template(curver, nil, queue_id), :any, 1)
      if message_row or curver.to_i == get_worker_version.to_i
        true
      else
        false
      end
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
      SkynetMessageQueue.connection.execute("delete from #{message_queue_table} where (tasktype='result' and expire_time < #{Time.now.to_i}) OR (expire_time BETWEEN 1 AND '#{Time.now.to_i - 7200}' and iteration = -1) OR (expire_time BETWEEN 1 AND '#{Time.now.to_i - 36000}')")
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
        :untaken_future_tasks => 0,
        :time                 => Time.now.to_f,
      }

      stats[:untaken_future_tasks] = SkynetWorkerQueue.connection.select_value(%{
        SELECT count(id)
        FROM #{message_queue_table}
        WHERE expire_time > #{Time.now.to_i} and tasktype = 'task' and payload_type = 'master'
      })

      stat_rows = SkynetWorkerQueue.connection.select_all(%{
        SELECT tasktype, payload_type, iteration, count(id) as number_of_tasks, expire_time
        FROM #{message_queue_table}
        WHERE expire_time <= #{Time.now.to_i}
        GROUP BY tasktype, payload_type, iteration
      })
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
          if row["iteration"].to_i == 0
            stats["untaken_#{type_of_tasks}".to_sym] += row["number_of_tasks"].to_i
            stats[:untaken_tasks] += row["number_of_tasks"].to_i
          elsif row["expire_time"].to_i < Time.now.to_i
            stats[:failed_tasks] += row["number_of_tasks"].to_i
          else
            stats["taken_#{type_of_tasks}".to_sym] += row["number_of_tasks"].to_i
            stats[:taken_tasks] += row["number_of_tasks"].to_i
          end
        end
      end

      stats[:time]    = Time.now.to_f - stats[:time]
      stats
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

    def find_next_message(template, payload_type, temperature=nil)
      conditions = template_to_conditions(template)
      temperature ||= temperature(payload_type)
      temperature_sql = (temperature > 1 ? " AND id % #{temperature.ceil} = #{rand(temperature).to_i} " : '')

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

    @@temperature ||= {}
    @@temperature[:task] ||= 1
    @@temperature[:master] ||= 1
    @@temperature[:any] ||= 1

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
