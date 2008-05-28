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

    options[:script_path] = Skynet::CONFIG[:LAUNCHER_PATH]
    
    if ARGV.detect {|a| a == 'console' }
      ARGV.delete('console')
      Skynet::Console.start
    elsif options[:worker_type] or ARGV.detect {|a| a =~ /worker_type/ }
      Skynet::Worker.start(options)
    else
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
