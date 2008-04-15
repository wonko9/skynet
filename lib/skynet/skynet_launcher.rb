# FIXME: should be a module
class Skynet
  include SkynetDebugger
  def self.start(options={})            
    if ARGV.detect {|a| a == 'console' }
      ARGV.delete('console')
      Skynet::Console.start
    elsif options[:worker_type] or ARGV.detect {|a| a =~ /worker_type/ }
      Skynet::Worker.start(options)
    else
      if Skynet::CONFIG[:SKYNET_LOG_DIR] == Skynet::DEFAULT_LOG_FILE_LOCATION
        puts "Logging to the default log: #{File.expand_path(Skynet::CONFIG[:SKYNET_LOG_FILE])}.  Set Skynet::CONFIG[:SKYNET_LOG_FILE] to change.\nYou will no longer see this warning once the Skynet::CONFIG[:SKYNET_LOG_FILE] is set."
      end
      Skynet::Manager.start(options)
    end
  end

  def self.new(options={})
    warn("Skynet.new is deprecated, please use Skynet.start instead")
    start(options)
  end
end
