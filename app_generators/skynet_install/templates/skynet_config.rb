# Skynet Configuration File.  Should be in APP_ROOT/config/skynet_config.rb  
# Start skynet from within your app root with 
# > skynet start

require 'rubygems'
require 'skynet'         

<% if in_rails -%>
# Load your rails app
if not defined?(RAILS_GEM_VERSION)
  require File.expand_path(File.dirname(__FILE__)) + '/../config/environment'
end
<% end -%>

Skynet::CONFIG[:SKYNET_LOG_LEVEL] = Logger::ERROR
<% if in_rails -%>
Skynet::CONFIG[:APP_ROOT]         = RAILS_ROOT
Skynet::CONFIG[:SKYNET_LOG_DIR]   = File.expand_path(Skynet::CONFIG[:APP_ROOT]+ "/log")
Skynet::CONFIG[:SKYNET_PID_DIR]   = File.expand_path(Skynet::CONFIG[:APP_ROOT] + "/log")
Skynet::CONFIG[:SKYNET_LOG_FILE]  = "skynet_#{RAILS_ENV}.log"
Skynet::CONFIG[:SKYNET_PID_FILE]  = "skynet_#{RAILS_ENV}.pid"
<% else -%>
Skynet::CONFIG[:SKYNET_LOG_DIR]   = File.expand_path(File.dirname(__FILE__) + "/../log") 
Skynet::CONFIG[:SKYNET_PID_DIR]   = File.expand_path(File.dirname(__FILE__) + "/../log") 
<% end -%>

<% if mysql -%>
<% if not in_rails -%>
# Use the mysql message queue adapter
Skynet::CONFIG[:MESSAGE_QUEUE_ADAPTER] = "Skynet::MessageQueueAdapter::Mysql"
Skynet::CONFIG[:MYSQL_HOST]            = "localhost"
Skynet::CONFIG[:MYSQL_USERNAME]        = "root"
Skynet::CONFIG[:MYSQL_PASSWORD]        = ""
Skynet::CONFIG[:MYSQL_DATABASE]        = "skynet"
<% else %>
# Use the mysql message queue adapter
Skynet::CONFIG[:MESSAGE_QUEUE_ADAPTER] = "Skynet::MessageQueueAdapter::Mysql"
<% end -%>
<% end -%>

# ==================================================================
# = Require any other libraries you want skynet to know about here =
# ==================================================================


# ===========================================
# = Set your own configuration options here =
# ===========================================
# You can also configure skynet with
# Skynet.configure(:SOME_CONFIG_OPTION => true, :SOME_OTHER_CONFIG => 3)