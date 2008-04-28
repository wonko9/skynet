require 'skynet'
Skynet::CONFIG[:SKYNET_LOG_DIR]   = File.expand_path("#{RAILS_ROOT}/log")
Skynet::CONFIG[:SKYNET_LOG_FILE]  = "skynet_#{RAILS_ENV}.log"
Skynet::CONFIG[:SKYNET_PID_DIR]   = File.expand_path("#{RAILS_ROOT}/log")
Skynet::CONFIG[:SKYNET_LOG_LEVEL] = Logger::ERROR
<% if mysql -%>
# Use the mysql message queue adapter
Skynet::CONFIG[:MESSAGE_QUEUE_ADAPTER] = "Skynet::MessageQueueAdapter::Mysql"
<% end %>