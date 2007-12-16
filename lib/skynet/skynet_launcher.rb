class Skynet
  include SkynetDebugger
  def self.new(*args)            
    if ARGV.detect {|a| a =~ /worker_type/ }
      Skynet::Worker.start
    else
      Skynet::Manager.start
    end
  end
end