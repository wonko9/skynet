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


    VALID_PAYLOAD_TYPES = ["master_or_task","master","task","error"]
    
    #superclass
    def check_task_version(task)
      ## IF this task isn't the right version and we find there's a new version, raise
      ## Make sure NOT to dequeue task if we raised above
      curver = get_worker_version
      raise Skynet::NewWorkerVersion.new if curver != task.version and curver != get_worker_version
    end

    #superclass
    def take_next_task(*args)
      message = dequeue_task(*args)
      raise InvalidMessage.new("Got a #{message.class}, expecting a Skynet::TaskMessage") unless message.is_a?(Skynet::TaskMessage)
      check_task_version(message)
      accept_unacknowledged_message(message)
      message
    end

    def take_result(*args)
      dequeue_result(*args)
    end
    
    #superclass
    def write_failover(message)
      failover_message = message.fallback_task_message
      write_task(failover_message) if failover_message
    end

    #superclass
    def write_task(message,timeout=nil)

      raise InvalidMessage.new("Got a #{message.class}, expecting a Skynet::TaskMessage") unless message.is_a?(Skynet::TaskMessage)

      # There are only two payload types, :task and :master
      tasktype = message.tasktype.to_s
      raise Error.new("#{tasktype} is not a valid payload type.  Only allowed types are #{VALID_PAYLOAD_TYPES.join(',')}") unless VALID_PAYLOAD_TYPES.include?(tasktype.to_s)
      enqueue_task(message)
      
      return true # wrote successfully
    end        
    
    #superclass
    def write_message(message,timeout=nil)
      write_task(message,timeout)
    end

    #superclass
    def write_result_for_task(task_message,result,timeout=nil)
      raise InvalidMessage.new("Got a #{message.class}, expecting a Skynet::TaskMessage") unless task_message.is_a?(Skynet::TaskMessage) or task_message.is_a?(Skynet::ErrorMessage)
      acknowledge(task_message)
      if task_message.is_a?(Skynet::ErrorMessage)
        message = task_message.error_message(error)

        # FIXME Do we always write the failover when there's an error?
        write_failover(task_message)
        write_error(message,timeout)
      else
        message = task_message.result_message(result)
        write_result(message,timeout)
      end
    end
    
    #superclass
    def write_result(message,timeout=nil)

      raise InvalidMessage.new("Got a #{message.class}, expecting a Skynet::ResultMessage") unless message.is_a?(Skynet::ResultMessage)

      ## It is assumed a queue was created for the original publisher of the task.  
      ## Here we'll add this task to that publisher's queue

      # First we'll make sure that task is acknowledged so it is taken off the Q permanently

      enqueue_result(message)
    end
    
    #superclass
    def write_error_for_task(task_message,error,timeout=nil)
      raise InvalidMessage.new("Got a #{message.class}, expecting a Skynet::TaskMessage") unless task_message.is_a?(Skynet::TaskMessage)
      message = task_message.error_message(error)
      write_error(message,timeout)
    end

    #superclass
    def write_error(message,error,timeout=nil)

      raise InvalidMessage.new("Got a #{message.class}, expecting a Skynet::ErrorMessage") unless message.is_a?(Skynet::ErrorMessage)

      ## Errors are forms of results and go back on the results queue
      enqueue_result(message)
    end
  end
end
