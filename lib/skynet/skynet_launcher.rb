class Skynet
  include SkynetDebugger
  def self.new(*args)            
    if ARGV.detect {|a| a =~ /worker_type/ }
      error "STARTING WORKER"
      Skynet::Worker.start
    else
      error "STARTING MANAGER"
      Skynet::Manager.start
    end
  end
end