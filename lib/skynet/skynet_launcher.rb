# FIXME: should be a module
class Skynet
  include SkynetDebugger
  def self.new(options={})            
    if ARGV.detect {|a| a == 'console' }
      ARGV.delete('console')
      Skynet::Console.start
    elsif options[:worker_type] or ARGV.detect {|a| a =~ /worker_type/ }
      Skynet::Worker.start(options)
    else
      Skynet::Manager.start(options)
    end
  end
end