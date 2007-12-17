$:.unshift File.dirname(__FILE__)
$:.unshift File.dirname(__FILE__) + '/skynet'

# path = File.expand_path(File.dirname(__FILE__))


require 'drb'
require 'skynet_guid_generator'
require 'skynet_logger'
require 'skynet_config'

Skynet::CONFIG[:SKYNET_PATH]    ||= File.expand_path(File.dirname(__FILE__) +"/..")
Skynet::CONFIG[:LAUNCHER_PATH]  ||= File.expand_path(ENV['_'])

require 'skynet_debugger'
require 'skynet_message'
require 'message_queue_adapters/message_queue_adapter'
require 'message_queue_adapters/tuple_space'
require "skynet_message_queue"
require 'skynet_job'
require 'skynet_worker'
require 'skynet_task'
require 'skynet_manager'
require 'skynet_tuplespace_server'
require 'skynet_ruby_extensions'
begin                       
  require 'active_record'
  require 'skynet_active_record_extensions'
  require 'message_queue_adapters/mysql'
rescue LoadError => e
end
require 'mapreduce_test'
require 'skynet_launcher'
require 'skynet_console'
