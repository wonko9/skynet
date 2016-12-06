class <%= migration_name %> < ActiveRecord::Migration
  def self.up
    create_table :skynet_message_queues do |t|
      t.column  :id,            "bigint unsigned primary key"
      t.column  :queue_id,      :integer, :default => 0
      t.column  :tran_id,       "bigint unsigned"
      t.column  :created_on,    :timestamp
      t.column  :updated_on,    :timestamp
      t.column  :tasktype,      :string
      t.column  :task_id,       'bigint unsigned'
      t.column  :job_id,        'bigint unsigned'
      t.column  :raw_payload,   "longtext"
      t.column  :payload_type,  :string
      t.column  :name,          :string
      t.column  :expiry,        :integer
      t.column  :expire_time,   "decimal(16,4)"
      t.column  :iteration,     :integer
      t.column  :version,       :integer
      t.column  :timeout,       "decimal(16,4)"
      t.column  :retry,         :integer, :default => 0
    end
    create_table :skynet_queue_temperature do |t|
      t.column  :id,            "bigint unsigned primary key"
      t.column  :queue_id,      :integer, :default => 0
      t.column  :updated_on,    :timestamp
      t.column  :count,         :integer, :default => 0
      t.column  :temperature,   "decimal(6,4) default 1"
      t.column  :type,          :string
    end
    add_index :skynet_message_queues, :job_id
    add_index :skynet_message_queues, :task_id
    add_index :skynet_message_queues, :tran_id, :unique => true
    add_index :skynet_message_queues, [:queue_id,:tasktype,:payload_type,:expire_time], :name => "index_skynet_mqueue_for_take"
    execute "insert into skynet_queue_temperature (queue_id,type) values (0,'master')"
    execute "insert into skynet_queue_temperature (queue_id,type) values (0,'any')"
    execute "insert into skynet_queue_temperature (queue_id,type) values (0,'task')"
  end

  def self.down
    drop_table :skynet_queue_temperature
    drop_table :skynet_message_queues
  end
end