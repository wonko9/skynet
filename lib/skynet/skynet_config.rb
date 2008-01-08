class Skynet       
  LOGDIR = "/var/log"
  
  CONFIG = {
    :ENABLE                            => true,
    :SOLO                              => false,
    :SKYNET_LOG_DIR                    => LOGDIR,
    :SKYNET_PID_DIR                    => "/tmp",
    :SKYNET_PIDS_FILE                  => "/tmp/skynet.pid",
    :SKYNET_LOG_FILE                   => STDOUT,
    :SKYNET_LOG_LEVEL                  => Logger::ERROR,
    :SKYNET_LOCAL_MANAGER_URL          => "druby://localhost:40000",
    :MESSAGE_QUEUE_ADAPTER             => "Skynet::MessageQueueAdapter::TupleSpace",
    # :TUPLESPACE_DRBURIS              => ["druby://localhost:47647"]
    :SERVER_HOSTS                      => ["localhost:7647"],
    # :MESSAGE_QUEUE_ADAPTER           => "Skynet::MessageQueueAdapter::Mysql",
    # :QUEUE_DATABASE                  => "skynet_queue",
    # :MYSQL_TEMPERATURE_CHANGE_SLEEP  => 40,
    # :MYSQL_MESSAGE_QUEUE_TAPLE       => "skynet_message_queues",
    # :MYSQL_MESSAGE_QUEUE_TEMP_CHECK_DELAY => 40,
    :NEXT_TASK_TIMEOUT                 => 60,
    :USE_RINGSERVER                    => true,
    :NUMBER_OF_WORKERS                 => 4,
    :WORKER_CHECK_DELAY                => 40,
    # :GUID_GENERATOR                  => nil,
    :PERCENTAGE_OF_TASK_ONLY_WORKERS   => 0.7,
    :PERCENTAGE_OF_MASTER_ONLY_WORKERS => 0.2,
    :MAX_RETRIES                        => 6,
    :DEFAULT_MASTER_RETRY              => 0,
    :DEFAULT_MAP_RETRY                 => 3,
    :DEFAULT_REDUCE_RETRY              => 3
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
      config[:ENABLE]              = true
      config[:SOLO]                = true
      config[:SKYNET_LOG_FILE]   ||= STDOUT
      config[:SKYNET_LOG_LEVEL]  ||= Logger::ERROR
      configure(config) do
        result = yield
      end
    rescue Exception => e
      error "Something bad happened #{e.inspect} #{e.backtrace.join("\n")}"
    end
    return result
  end
  
  # Skynet has many global configuration options.
  # You can access specific options via Skynet::CONFIG[:OPTION] = ?
  # You can set many options via                                          
  #
  #    Skynet.configure(:OPTION => option, :ANOTHEROPTION => anotheroption)
  #
  # If you want specific configuration to only apply to a block of code you can pass configure a block
  #
  #    Skynet.configure(:SOMEOPTION => 'value') do
  #       run code here
  #    end
  #
  # Config Options and current defaults:
  #  Skynet.configure(
  #   :ENABLE                            => true,
  #   :SOLO                              => false,
  #   :SKYNET_LOG_DIR                    => LOGDIR,
  #   :SKYNET_PID_DIR                    => "/tmp",
  #   :SKYNET_PIDS_FILE                  => "/tmp/skynet.pid",
  #   :SKYNET_LOG_FILE                   => STDOUT,
  #   :SKYNET_LOG_LEVEL                  => Logger::ERROR,
  #   :SKYNET_LOCAL_MANAGER_URL          => "druby://localhost:40000",
  #   :TUPLESPACE_DRBURIS                => ["druby://localhost:47647"]
  #   :USE_RINGSERVER                    => true,
  #   :SERVER_HOSTS                      => ["localhost:7647"],
  #   :MESSAGE_QUEUE_ADAPTER             => "Skynet::MessageQueueAdapter::TupleSpace",
  #   :QUEUE_DATABASE                    => "skynet_queue",
  #   :MYSQL_TEMPERATURE_CHANGE_SLEEP    => 40,
  #   :MYSQL_QUEUE_TEMP_POW              => 0.6,
  #   :MYSQL_MESSAGE_QUEUE_TAPLE         => "skynet_message_queues",
  #   :MYSQL_MESSAGE_QUEUE_TEMP_CHECK_DELAY => 40,
  #   :NEXT_TASK_TIMEOUT                 => 60,
  #   :WORKER_CHECK_DELAY                => 40,
  #   :GUID_GENERATOR                    => nil,
  #   :NUMBER_OF_WORKERS                 => 4,
  #   :PERCENTAGE_OF_TASK_ONLY_WORKERS   => 0.7,
  #   :PERCENTAGE_OF_MASTER_ONLY_WORKERS => 0.2,
  #   :MAX_RETRY_TIMES                   => 6,
  #   :DEFAULT_MASTER_RETRY_TIMES        => 0,
  #   :DEFAULT_MAP_RETRY_TIMES           => 3,
  #   :DEFAULT_REDUCE_RETRY_TIMES        => 3
  
  #  )
  #
  # ENABLE turns Skynet on or off.
  #
  # SOLO turns on SOLO mode where Skynet can run in the current process without workers.  
  # Its almost a Skynet emulation mode
  #
  # MESSAGE_QUEUE_ADAPTER
  # Skynet comes with 2 message queue adaptors 
  #   Skynet::MessageQueueAdapter::TupleSpace 
  #   Skynet::MessageQueueAdapter::Mysql
  # The default is TupleSpace which is an in memory database.
  # The mysql MQ takes running a migration that comes with skynet_install
  #
  # The following only apply to the TupleSpace adapter
  #   :TUPLESPACE_DRBURIS                => ["druby://localhost:47647"]
  #   :USE_RINGSERVER                    => true,
  #   :SERVER_HOSTS                      => ["localhost:7647"],
  #
  # The following only apply to the Mysql adapter
  #   :QUEUE_DATABASE                    => "skynet_queue",
  #   :MYSQL_TEMPERATURE_CHANGE_SLEEP    => 40,
  #   :MYSQL_QUEUE_TEMP_POW              => 0.6,
  #   :NEXT_TASK_TIMEOUT                 => 60,
  #   :WORKER_CHECK_DELAY                => 40,  
  #   :MYSQL_MESSAGE_QUEUE_TAPLE         => "skynet_message_queues",
  #   :MYSQL_MESSAGE_QUEUE_TEMP_CHECK_DELAY => 40,

  class Config     
  end
end