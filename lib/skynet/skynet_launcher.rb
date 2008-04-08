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
      Skynet::Manager.start(options)
    end
  end

  def self.new(options={})
    warn("Skynet.new is deprecated, please use Skynet.start instead")
    start(options)
  end
end
