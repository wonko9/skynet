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
  
  def debug_header
    self.class.debug_header
  end
  
  module ClassMethods
    
    def debug_class_desc
      self.to_s
    end
    
    def debug_header
      t = Time.now
      "##{$$} #{t.strftime("%Y-%m-%d %H:%M:%S")}.#{t.usec} <#{debug_class_desc}>"
    end
    
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
  
  end

end