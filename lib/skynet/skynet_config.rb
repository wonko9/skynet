class Skynet       
  LOGDIR = "/var/log"
  
  CONFIG = {
    :ENABLE                         => true,
    :SOLO                           => false,
    :SKYNET_LOG_DIR                 => LOGDIR,
    :SKYNET_PID_DIR                 => "/tmp",
    :SKYNET_PIDS_FILE               => "/tmp/skynet.pid",
    :SKYNET_LOG_FILE                => STDOUT,
    :SKYNET_LOG_LEVEL               => Logger::ERROR,
    :SKYNET_LOCAL_MANAGER_URL       => "druby://localhost:40000",
    :MESSAGE_QUEUE_ADAPTER          => "Skynet::MessageQueueAdapter::TupleSpace",
    # :TUPLESPACE_DRBURIS             => ["druby://localhost:47647"]
    # :MESSAGE_QUEUE_ADAPTER          => "Skynet::MessageQueueAdapter::Mysql",
    # :QUEUE_DATABASE                 => "skynet_queue",
    # :MYSQL_TEMPERATURE_CHANGE_SLEEP   => 40,
    :NEXT_TASK_TIMEOUT              => 60,
    :USE_RINGSERVER                 => true,
    :SERVER_HOSTS                   => ["localhost:7647"],
    :NUMBER_OF_WORKERS              => 4,
    :WORKER_CHECK_DELAY             => 40,
    # :GUID_GENERATOR                 => nil,
    :PERCENTAGE_OF_TASK_ONLY_WORKERS    => 0.7,
    :PERCENTAGE_OF_MASTER_ONLY_WORKERS  => 0.2
  } unless defined?(CONFIG)
  
  
  def self.configure(config={})
    old_config = CONFIG.dup
    config.each {|k,v| CONFIG[k] = v}
    if block_given?
      ret = yield
      CONFIG.keys.each do |key|
        CONFIG.delete(key)
      end
      old_config.each {|k,v| CONFIG[k] = v}
      ret
    end
  end

  def self.solo(config = {}) 
    raise Skynet::Error.new("You provide a code block to Skynet.solo") unless block_given?
    result = nil
    Skynet::Logger.log = nil
    begin
      config[:ENABLE] = true
      config[:SOLO] = true
      config[:SKYNET_LOG_FILE] ||= STDOUT
      config[:SKYNET_LOG_LEVEL] ||= Logger::ERROR
      configure(config) do
        result = yield
      end
    rescue Exception => e
      error "Something bad happened #{e.inspect} #{e.backtrace.join("\n")}"
    end
    return result
  end
end