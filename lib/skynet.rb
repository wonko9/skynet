$:.unshift File.dirname(__FILE__)
$:.unshift File.dirname(__FILE__) + '/skynet'

# path = File.expand_path(File.dirname(__FILE__))


require 'drb'
require 'skynet_guid_generator'
require 'skynet_logger'
require 'skynet_config'
require 'timeout'    

Skynet::CONFIG[:SKYNET_PATH]    ||= File.expand_path(File.dirname(__FILE__) +"/..")
# Skynet::CONFIG[:LAUNCHER_PATH]  ||= File.expand_path(ENV['_'])

require 'skynet_debugger'
require 'skynet_message'
require 'message_queue_adapters/message_queue_adapter'
require 'message_queue_adapters/tuple_space'
require 'worker_queue_adapters/tuple_space'
require "skynet_message_queue"
require 'skynet_partitioners'
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
require 'mapreduce_helper'


begin
  require 'fastthread'
rescue LoadError
  # puts 'fastthread not installed, using thread instead'
  require 'thread'
end

class Skynet

  # kinda like system() but gives me back a pid
  def self.fork_and_exec(command)
    sleep 0.01  # remove contention on manager drb object
    log = Skynet::Logger.get
    info "executing /bin/sh -c \"#{command}\""
    pid = fork do
      close_files
      exec("/bin/sh -c \"#{command}\"")
      exit
    end
    Process.detach(pid)
    pid
  end

  # close open file descriptors starting with STDERR+1
  def self.close_files(from=3, to=50)
    close_console
    (from .. to).each do |fd|
      IO.for_fd(fd).close rescue nil
     end
  end

  def self.close_console
    STDIN.reopen "/dev/null"
    STDOUT.reopen "/dev/null", "a"
    STDERR.reopen STDOUT 
  end

end
