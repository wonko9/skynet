# Skynet Configuration File.  Should be in APP_ROOT/config/skynet_config.rb  
# Start skynet from within your app root with 
# $ skynet start

require 'rubygems'
require 'skynet'         

Skynet::CONFIG[:SKYNET_LOG_LEVEL] = Logger::ERROR
Skynet::CONFIG[:SKYNET_LOG_DIR]   = File.expand_path(File.dirname(__FILE__) + "/../log") 
Skynet::CONFIG[:SKYNET_PID_DIR]   = File.expand_path(File.dirname(__FILE__) + "/../log") 

# ==================================================================
# = Require any other libraries you want skynet to know about here =
# ==================================================================

# require all ruby files in lib/
Dir["#{File.dirname(__FILE__)}" + "/../lib/**/*.rb"].each do |file|
  require file
end

# ===========================================
# = Set your own configuration options here =
# ===========================================
# You can also configure skynet with
# Skynet.configure(:SOME_CONFIG_OPTION => true, :SOME_OTHER_CONFIG => 3)

