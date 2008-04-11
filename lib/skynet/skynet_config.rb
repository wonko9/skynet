class Skynet       
  LOGDIR = "/var/log"
  
  CONFIG = {
    :ENABLE                               => true,
    :SOLO                                 => false,
    :SKYNET_LOG_DIR                       => LOGDIR,
    :SKYNET_PID_DIR                       => "/tmp",
    :SKYNET_PIDS_FILE                     => "/tmp/skynet.pid",
    :SKYNET_LOG_FILE                      => "skynet.log",
    :SKYNET_LOG_LEVEL                     => Logger::INFO,
    :SKYNET_LOCAL_MANAGER_URL             => "druby://localhost:40000",
    :MESSAGE_QUEUE_ADAPTER                => ("Skynet::MessageQueueAdapter::TupleSpace" || "Skynet::MessageQueueAdapter::Mysql"),
    :TS_USE_RINGSERVER                    => true,
    :TS_DRBURIS                           => ["druby://localhost:47647"],   # If you do not use RINGSERVER, you must specifiy the DRBURI
    :TS_SERVER_HOSTS                      => ["localhost:7647"],
    :TS_SERVER_START_DELAY                => 10,
    # :MYSQL_QUEUE_DATABASE                 => "skynet_queue",
    :MYSQL_TEMPERATURE_CHANGE_SLEEP       => 40,
    :MYSQL_MESSAGE_QUEUE_TAPLE            => "skynet_message_queues",
    :MYSQL_MESSAGE_QUEUE_TEMP_CHECK_DELAY => 40,
    :MYSQL_NEXT_TASK_TIMEOUT              => 60,
    :MYSQL_ADAPTER                        => "mysql",
    :MYSQL_HOST                           => "localhost",
    :MYSQL_DATABASE                       => "skynet",
    :MYSQL_USERNAME                       => "root",
    :MYSQL_PASSWORD                       => "",
    :NUMBER_OF_WORKERS                    => 4,
    :WORKER_CHECK_DELAY                   => 40,
    :WORKER_MAX_MEMORY                    => 500,
    :WORKER_MAX_PROCESSED                 => 1000,
    :WORKER_VERSION_CHECK_DELAY           => 30,
    # :GUID_GENERATOR                     => nil,
    :PERCENTAGE_OF_TASK_ONLY_WORKERS      => 0.7,
    :PERCENTAGE_OF_MASTER_ONLY_WORKERS    => 0.2,
    :MAX_RETRIES                          => 6,
    :DEFAULT_MASTER_RETRY                 => 0,
    :DEFAULT_MAP_RETRY                    => 3,
    :DEFAULT_REDUCE_RETRY                 => 3,
    :DEFAULT_KEEP_MAP_TASKS               => 1,
    :DEFAULT_KEEP_REDUCE_TASKS            => 1,
    :MESSAGE_QUEUES                       => ['first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth', 'nineth']
  } unless defined?(CONFIG)
                           
  def self.silent
    if block_given?
      Skynet.configure(:SKYNET_LOG_LEVEL => 10) do
        yield
      end
    else
      raise Error.new("Please provide a block to Skynet.silent")
    end
  end
  
  def self.configure(config={})
    old_config = CONFIG.dup
    config.each {|k,v| CONFIG[k] = v}
    Skynet::Logger.log = nil
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
  #   :ENABLE                               => true,
  #   :SOLO                                 => false,
  #   :SKYNET_LOG_DIR                       => LOGDIR,
  #   :SKYNET_PID_DIR                       => "/tmp",
  #   :SKYNET_PIDS_FILE                     => "/tmp/skynet.pid",
  #   :SKYNET_LOG_FILE                      => STDOUT,
  #   :SKYNET_LOG_LEVEL                     => Logger::ERROR,
  #   :SKYNET_LOCAL_MANAGER_URL             => "druby://localhost:40000",
  #   :MESSAGE_QUEUE_ADAPTER                => "Skynet::MessageQueueAdapter::TupleSpace",
  #   :TS_DRBURIS                           => ["druby://localhost:47647"]
  #   :TS_USE_RINGSERVER                    => true,
  #   :TS_SERVER_HOSTS                      => ["localhost:7647"],
  #   :TS_SERVER_START_DELAY                => 10,
  #   :MYSQL_QUEUE_DATABASE                 => "skynet_queue",
  #   :MYSQL_TEMPERATURE_CHANGE_SLEEP       => 40,
  #   :MYSQL_QUEUE_TEMP_POW                 => 0.6,
  #   :MYSQL_MESSAGE_QUEUE_TAPLE            => "skynet_message_queues",
  #   :MYSQL_MESSAGE_QUEUE_TEMP_CHECK_DELAY => 40,
  #   :MYSQL_NEXT_TASK_TIMEOUT              => 60,
  #   :WORKER_CHECK_DELAY                   => 40,
  #   :GUID_GENERATOR                       => nil,
  #   :NUMBER_OF_WORKERS                    => 4,
  #   :PERCENTAGE_OF_TASK_ONLY_WORKERS      => 0.7,
  #   :PERCENTAGE_OF_MASTER_ONLY_WORKERS    => 0.2,
  #   :MAX_RETRIES                          => 6,
  #   :DEFAULT_MASTER_RETRY                 => 0,
  #   :DEFAULT_MAP_RETRY                    => 3,
  #   :DEFAULT_REDUCE_RETRY                 => 3
  #   :KEEP_MAP_TASKS                       => false,
  #   :KEEP_REDUCE_TASKS                    => false
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
  #   :TS_DRBURIS                           => ["druby://localhost:47647"]
  #   :TS_USE_RINGSERVER                    => true,
  #   :TS_SERVER_HOSTS                      => ["localhost:7647"],
  #
  # The following only apply to the Mysql adapter
  #   :MYSQL_QUEUE_DATABASE                 => "skynet_queue",
  #   :MYSQL_TEMPERATURE_CHANGE_SLEEP       => 40,
  #   :MYSQL_QUEUE_TEMP_POW                 => 0.6,
  #   :MYSQL_MESSAGE_QUEUE_TAPLE            => "skynet_message_queues",
  #   :MYSQL_MESSAGE_QUEUE_TEMP_CHECK_DELAY => 40,
  #   :MYSQL_NEXT_TASK_TIMEOUT              => 60,

  class Config     
    include Enumerable

    def each
      Skynet::CONFIG.each {|k,v| yield k,v}
    end

    def add_message_queue(queue_name)
      self.message_queues << queue_name
    end
    
    def queue_id_by_name(queue_name)
      if Skynet::CONFIG[:MESSAGE_QUEUES].index(queue_name)
        return Skynet::CONFIG[:MESSAGE_QUEUES].index(queue_name)
      else     
        raise Skynet::Error("#{queue_name} is not a valid queue")
      end
    end       
    
    def queue_name_by_id(queue_id)               
      queue_id = queue_id.to_i
      if Skynet::CONFIG[:MESSAGE_QUEUES][queue_id]
        return Skynet::CONFIG[:MESSAGE_QUEUES][queue_id]
      else     
        raise Skynet::Error("#{queue_id} is not a valid queue_id")
      end
    end
    
    
    
    def method_missing(name, *args)
      name = name.to_s.upcase.to_sym
      if name.to_s =~ /^(.*)=$/
        name = $1.to_sym
        Skynet::CONFIG[name] = args.first
      else
        Skynet::CONFIG[name]
      end
    end
  end
end
