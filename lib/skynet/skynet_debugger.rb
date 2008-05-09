# ==SkynetDebugger
# The SkynetDebugger is a module you can include in any of your classes that will give you easy access
# to the Skynet::Logger.  including SkynetDebugger gives you a number of logging methods.  Each logging method
# lets you pass a message as well as an optional number of objects which will be pretty_printed after your message.
# Log lines print with their log level, PID, time, class, and message.  eg.
#
#   [WARN] #78002 2008-04-11 14:17:15.363167 <WORKER-78002> Exiting... 
#
# You can set the log_level and log_file with  (See Skynet::Config)
#  Skynet::CONFIG[:SKYNET_LOG_FILE]
#  Skynet::CONFIG[:SKYNET_LOG_LEVEL]
#
# Possible log levels include
#  Logger::DEBUG 
#  Logger::INFO
#  Logger::WARN
#  Logger::ERROR 
#  Logger::FATAL 
#
# ==Methods
#  log - returns the Skynet::Logger
#
#  debug(msg,*objects_to_inspect)
#
#  info(msg,*objects_to_inspect)
#
#  warn(msg,*objects_to_inspect)
#
#  error(msg,*objects_to_inspect)
#
#  fatal(msg,*objects_to_inspect)
# 
#  printlog(msg,*objects_to_inspect)  #printlog will ALWAYS print to the log as log level [LOG] regardless of the LOG_LEVEL
module SkynetDebugger
  
  def self.included(base)
    base.extend ClassMethods
  end

  def log
    self.class.log
  end  

  def args_pp(*args)
    self.class.args_pp(*args)
  end

  def debug(*args)
    self.class.debug(*args)
  end

  def info(*args)
    self.class.info(*args)
  end

  def warn(*args)
    self.class.warn(*args)
  end

  def error(*args)
    self.class.error(*args)
  end

  def fatal(*args)
    self.class.fatal(*args)
  end          

  def printlog(*args)
    self.class.printlog(*args)
  end          
  
  def debug_header
    self.class.debug_header
  end
  
  def stderr(*args)
    self.class.stderr
  end

  def stdout(*args)
    self.class.stdout
  end
  
  module ClassMethods
    
    def debug_class_desc
      self.to_s
    end
    
    def debug_header
      t = Time.now
      "##{$$} #{t.strftime("%Y-%m-%d %H:%M:%S")}.#{t.usec} <#{debug_class_desc}>"
    end
    
    # log
    def log
      Skynet::Logger.get
    end

    def args_pp(*args)
      "#{args.length > 0 ? args.pretty_print_inspect : ''}"
    end      

    def debug(msg,*args)
      log.debug "[DEBUG] #{debug_header} #{msg} #{args_pp(*args)}"
    end

    def info(msg, *args)
      log.info "[INFO] #{debug_header} #{msg} #{args_pp(*args)}"
    end

    def warn(msg, *args)
      log.warn "[WARN] #{debug_header} #{msg} #{args_pp(*args)}"
    end

    def error(msg, *args)
      log.error "[ERROR] #{debug_header} #{msg} #{args_pp(*args)}"
    end

    def fatal(msg, *args)
      log.fatal "[FATAL] #{debug_header} #{msg} #{args_pp(*args)}"
    end

    def printlog(msg, *args)
      log.unknown "[LOG] #{debug_header} #{msg} #{args_pp(*args)}"
    end

    def stderr(msg, *args)
      $stderr.puts "#{debug_header} #{msg} #{args_pp(*args)}"
    end

    def stdout(msg, *args)
      $stdout.puts "#{debug_header} #{msg} #{args_pp(*args)}"
    end
  
  end

end