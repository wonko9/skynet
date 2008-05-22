# FIXME: should be a module
class Skynet
  include SkynetDebugger
  def self.start(options={})            
    begin
      mq = Skynet::MessageQueue.new
    rescue Skynet::ConnectionError
      if Skynet::MessageQueue.adapter == :tuplespace
        cmd = "skynet_tuplespace_server --logfile=#{Skynet.config.logfile_location} --piddir=#{Skynet.config.skynet_pid_dir} --use_ringserver=#{Skynet.config.ts_use_ringserver} --drburi=#{Skynet.config.ts_drburi} start"
        pid = fork do
          exec(cmd)
        end
        sleep Skynet::CONFIG[:TS_SERVER_START_DELAY]
      end
    end

    if ARGV.detect {|a| a == 'console' }
      ARGV.delete('console')
      Skynet::Console.start
    elsif options[:worker_type] or ARGV.detect {|a| a =~ /worker_type/ }
      Skynet::Worker.start(options)
    else
      if Skynet::CONFIG[:SKYNET_LOG_DIR] == Skynet::DEFAULT_LOG_FILE_LOCATION
        puts "Logging to the default log: #{File.expand_path(Skynet::CONFIG[:SKYNET_LOG_FILE])}.  Set Skynet::CONFIG[:SKYNET_LOG_FILE] to change.\nYou will no longer see this warning once the Skynet::CONFIG[:SKYNET_LOG_FILE] is set."
      end                          
      if ARGV.include?('stop')
        Skynet::Manager.stop(options)
      else
        options["daemonize"] = true if ARGV.include?('start')      
        Skynet::Manager.start(options)
      end
    end
  end

  def self.new(options={})
    warn("Skynet.new is deprecated, please use Skynet.start instead")
    start(options)
  end
end
