# This file is auto-generated from the current state of the database. Instead of editing this file, 
# please use the migrations feature of ActiveRecord to incrementally modify your database, and
# then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your database schema. If you need
# to create the application database on another system, you should be using db:schema:load, not running
# all the migrations from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended to check this file into your version control system.

ActiveRecord::Schema.define(:version => 3) do

  create_table "skynet_message_queues", :force => true do |t|
    t.integer  "queue_id",                                                  :default => 0
    t.integer  "tran_id",      :limit => 20
    t.datetime "created_on"
    t.datetime "updated_on"
    t.string   "tasktype"
    t.integer  "task_id",      :limit => 20
    t.integer  "job_id",       :limit => 20
    t.text     "raw_payload"
    t.string   "payload_type"
    t.string   "name"
    t.integer  "expiry"
    t.decimal  "expire_time",                :precision => 16, :scale => 4
    t.integer  "iteration"
    t.integer  "version"
    t.decimal  "timeout",                    :precision => 16, :scale => 4
    t.integer  "retry",                                                     :default => 0
  end

  add_index "skynet_message_queues", ["tran_id"], :name => "index_skynet_message_queues_on_tran_id", :unique => true
  add_index "skynet_message_queues", ["job_id"], :name => "index_skynet_message_queues_on_job_id"
  add_index "skynet_message_queues", ["task_id"], :name => "index_skynet_message_queues_on_task_id"
  add_index "skynet_message_queues", ["queue_id", "tasktype", "payload_type", "expire_time"], :name => "index_skynet_mqueue_for_take"

  create_table "skynet_queue_temperature", :force => true do |t|
    t.integer  "queue_id",                                  :default => 0
    t.datetime "updated_on"
    t.integer  "count",                                     :default => 0
    t.decimal  "temperature", :precision => 6, :scale => 4
    t.string   "type"
  end

  create_table "skynet_worker_queues", :force => true do |t|
    t.integer  "queue_id",                                                   :default => 0
    t.datetime "created_on"
    t.datetime "updated_on"
    t.string   "tasktype"
    t.string   "tasksubtype"
    t.integer  "worker_id",     :limit => 20
    t.string   "hostname"
    t.integer  "process_id"
    t.integer  "job_id",        :limit => 20
    t.integer  "task_id",       :limit => 20
    t.integer  "iteration"
    t.string   "name"
    t.string   "map_or_reduce"
    t.decimal  "started_at",                  :precision => 16, :scale => 4
    t.integer  "version"
    t.integer  "processed"
    t.decimal  "timeout",                     :precision => 16, :scale => 4
  end

  add_index "skynet_worker_queues", ["worker_id"], :name => "index_skynet_worker_queues_on_worker_id", :unique => true
  add_index "skynet_worker_queues", ["hostname", "process_id"], :name => "index_skynet_worker_queues_on_hostname_and_process_id"

  create_table "user_favorites", :force => true do |t|
    t.integer  "user_id"
    t.string   "favorite"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "users", :force => true do |t|
    t.string   "name"
    t.string   "password"
    t.string   "favorites"
    t.string   "email"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

end
