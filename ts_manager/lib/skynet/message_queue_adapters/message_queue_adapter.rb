class Skynet
  
  class Error < StandardError
  end
  
  class RequestExpiredError < Skynet::Error
	end
	
	class InvalidMessage < Skynet::Error
  end
  
  class AbstractClassError < Skynet::Error
  end
	
  class MessageQueueAdapter

    def list_results(data,timeout=nil)
      raise AbstractClassError.new("You must implement list_results in a subclass.")
    end

    def list_tasks(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end

    def take_next_task(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end

    def write_message(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end

    def write_result(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end

    def take_result(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end

    def write_error(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end            

    def write_worker_status(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end

    def take_worker_status(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end

    def read_all_worker_statuses(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end

    def get_worker_version(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end

    def set_worker_version(template,timeout=nil)
      raise AbstractClassError.new("You must implement method in a subclass.")
    end
      
    def clear_outstanding_tasks
      raise AbstractClassError.new("You must implement clear_outstanding_tasks in a subclass.")
    end  

  end
end
