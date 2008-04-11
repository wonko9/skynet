# http://darwinweb.net/article/Undoing_Rails_Monkey_Patch_To_Logger

require 'logger'

class Skynet  

  class Error < StandardError
  end

  class Logger < ::Logger
    if respond_to?(:format_message)
      alias format_message old_format_message
    end
    
    @@log = nil
    
    def self.get
      if not @@log
        @@log = self.new(Skynet::CONFIG[:SKYNET_LOG_FILE])
        @@log.level = Skynet::CONFIG[:SKYNET_LOG_LEVEL]
      end  
      @@log
    end
    
    def self.log=(log)
      @@log = log
    end
  end
    
  # This module can be mixed in to add logging methods to your class.
  module Loggable
    def debug
      log = Skynet::Logger.get
    end
    
    def info
      log = Skynet::Logger.get
    end
    
    def warn
      log = Skynet::Logger.get
    end
    
    def error
      log = Skynet::Logger.get
    end
    
    def fatal
      log = Skynet::Logger.get
    end
    
    def unknown
      log = Skynet::Logger.get
    end
  end
end
