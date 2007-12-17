class Skynet
  include SkynetDebugger
  def self.new(options={})            
    if options[:worker_type] or ARGV.detect {|a| a =~ /worker_type/ }
      Skynet::Worker.start(options)
    else
      Skynet::Manager.start(options)
    end
  end
end