class <%= migration_name %> < ActiveRecord::Migration
  def self.up
    create_table :skynet_worker_queues do |t|
      t.column  :id,            "bigint unsigned primary key"
      t.column  :created_on,    :timestamp
      t.column  :updated_on,    :timestamp
      t.column  :tasktype,      :string
      t.column  :tasksubtype,   :string
      t.column  :worker_id,     'bigint unsigned'
      t.column  :hostname,      :string
      t.column  :process_id,    :integer
      t.column  :job_id,        'bigint unsigned'
      t.column  :task_id,       'bigint unsigned'
      t.column  :iteration,     :integer
      t.column  :name,          :string
      t.column  :map_or_reduce, :string
      t.column  :started_at,    "decimal(16,4)"
      t.column  :version,       :integer
      t.column  :processed,     :integer
      t.column  :timeout,       "decimal(16,4)"            
    end                                 
    create_table :skynet_message_queues do |t|
      t.column  :id,            "bigint unsigned primary key"
      t.column  :created_on,    :timestamp
      t.column  :updated_on,    :timestamp
      t.column  :tasktype,      :string
      t.column  :task_id,       'bigint unsigned'
      t.column  :job_id,        'bigint unsigned'
      t.column  :raw_payload,   :text
      t.column  :payload_type,  :string
      t.column  :name,          :string
      t.column  :expiry,        :integer
      t.column  :expire_time,   "decimal(16,4)"
      t.column  :iteration,     :integer
      t.column  :version,       :integer
      t.column  :timeout,       "decimal(16,4)"
      t.column  :retry          :integer, :default => 0
    end                                   
    create_table :skynet_queue_temperature do |t|
      t.column  :id,            "bigint unsigned primary key"
      t.column  :updated_on,    :timestamp
      t.column  :count,         :integer, :default => 0
      t.column  :temperature,   "decimal(6,4) default 1"
      t.column  :type,          :string
    end
    add_index :skynet_message_queues, [:job_id]
    add_index :skynet_message_queues, :task_id
    add_index :skynet_message_queues, [:tasktype,:payload_type,:expire_time], :name => "index_skynet_mqueue_for_take"
    add_index :skynet_worker_queues, [:hostname, :process_id]
    add_index :skynet_worker_queues, :worker_id, :unique=> true
    execute "insert into skynet_queue_temperature (type) values ('master')"
    execute "insert into skynet_queue_temperature (type) values ('any')"
    execute "insert into skynet_queue_temperature (type) values ('task')"
  end

  def self.down
    drop_table :skynet_worker_queues
    drop_table :skynet_queue_temperature
    drop_table :skynet_message_queues
  end
end