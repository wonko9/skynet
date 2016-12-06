# Skynet Configuration File.  Should be in APP_ROOT/config/skynet_config.rb
# Start skynet from within your app root with
# > skynet start

require 'rubygems'
require 'skynet'

# Load your rails app
if not defined?(RAILS_GEM_VERSION)
  require File.expand_path(File.dirname(__FILE__)) + '/../config/environment'
end

Skynet::CONFIG[:SKYNET_LOG_LEVEL] = Logger::ERROR
Skynet::CONFIG[:APP_ROOT]         = RAILS_ROOT
Skynet::CONFIG[:SKYNET_LOG_DIR]   = File.expand_path(Skynet::CONFIG[:APP_ROOT]+ "/log")
Skynet::CONFIG[:SKYNET_PID_DIR]   = File.expand_path(Skynet::CONFIG[:APP_ROOT] + "/log")
Skynet::CONFIG[:SKYNET_LOG_FILE]  = "skynet_#{RAILS_ENV}.log"
Skynet::CONFIG[:SKYNET_PID_FILE]  = "skynet_#{RAILS_ENV}.pid"


# Use the mysql message queue adapter
Skynet::CONFIG[:MESSAGE_QUEUE_ADAPTER] = "Skynet::MessageQueueAdapter::Mysql"

# ==================================================================
# = Require any other libraries you want skynet to know about here =
# ==================================================================


# ===========================================
# = Set your own configuration options here =
# ===========================================
# You can also configure skynet with
# Skynet.configure(:SOME_CONFIG_OPTION => true, :SOME_OTHER_CONFIG => 3)

Skynet::CONFIG[:SKYNET_LOG_LEVEL]  = Logger::INFO
Skynet::CONFIG[:TS_USE_RINGSERVER] = false
