require File.dirname(__FILE__) + '/test_helper.rb'

class MessageQueueAdapterTest < Test::Unit::TestCase
  
  attr_reader :mq

  Skynet.configure(
    :ENABLE                   => false,
    :SKYNET_LOG_FILE          => STDOUT,
    :SKYNET_LOG_LEVEL         => Logger::ERROR,
    :TS_DRBURI                => "druby://localhost:40088",
    :SKYNET_LOCAL_MANAGER_URL => "druby://localhost:40000",
    :MESSAGE_QUEUE_ADAPTER    => "Skynet::MessageQueueAdapter::TupleSpace"
    # :MESSAGE_QUEUE_ADAPTER    => "Skynet::MessageQueueAdapter::SingleProcess"
  )      

  def setup
        
    @ts ||= Rinda::TupleSpace.new
    @@tss ||= DRb.start_service(Skynet::CONFIG[:TS_DRBURI], @ts)

    @mq = Skynet::MessageQueue.new    

    @mq.mq.flush

    @worker_message = Skynet::TaskMessage.new(
      :tasktype    => :task, 
      :drburi      => "localhost", 
      :job_id      => 1,
      :task_id     => 2,
      :task        => "task",
      :timeout     => 20, 
      :result      => nil, 
      :expire_time => 0,
      :iteration   => 0,
      :name        => "test",       
      :version     => 1,
      :retry       => 3,
      :queue_id    => 0
    )
  end
     
  def test_write_and_take_next_task    
    assert mq.write_task(@worker_message)
    assert_equal @worker_message.to_h, mq.take_next_task(@worker_message.version,:task).to_h
  end
  
  def test_task_failover
    message = @worker_message.clone
    message.timeout=0.4
    assert mq.write_message(message)
    taken = mq.take_next_task(message.version,message.tasktype)
    assert_equal message.to_h, taken.to_h
    sleep 1
    assert_equal 2, mq.take_next_task(message.version,message.tasktype).task_id
  end

  def test_task_failover_no_retry
    message = @worker_message.clone
    message.retry = false
    message.timeout=0.4
    message.name = "hi"
    assert mq.write_task(message)
    assert_equal message.to_h, mq.take_next_task(message.version,message.tasktype).to_h
    sleep 1.5         
    notask = false
    begin 
      task = mq.take_next_task(message.version,message.tasktype).task_id
    rescue Skynet::RequestExpiredError
      notask = true
    end
    assert notask
  end

  def test_task_retries
    message = @worker_message.clone
    message.timeout=0.4
    message.retry = 2
    assert mq.write_message(message)
    assert_equal message.to_h, mq.take_next_task(message.version,message.tasktype).to_h
    sleep 1
    assert_equal 2, mq.take_next_task(message.version,message.tasktype).task_id
    sleep 1
    assert_equal 2, mq.take_next_task(message.version,message.tasktype).task_id
    notask = false
    begin 
      mq.take_next_task(message.version,message.tasktype).task_id
    rescue Skynet::RequestExpiredError
      notask = true
    end
    assert notask
  end

  def test_write_and_take_result
    assert mq.write_task(@worker_message)
    message = mq.take_next_task(@worker_message.version,:task)
    assert_equal @worker_message, message

    set_result = {:blah => ['hi']}
    
    mq.write_result_for_task(message,set_result,20)
    result = mq.take_result(message.job_id)
    assert_equal set_result, result.result

    notask = false
    begin 
      mq.take_next_task(message.version,message.tasktype).task_id
    rescue Skynet::RequestExpiredError
      notask = true
    end
    assert notask
  end


  ## do we want to retry if there's a basic error?  What are the circumstances of a retry?
  ## There are exceptions thrown IN execution of the task
  ## such as timeout errors executing the task
  ## Then there are errors in skynet during execution
  ## I thought about having 2 error codes, a skynet_error_code and a task_error_code which
  ## could be specified by the developer
  def test_write_and_take_error
    assert mq.write_task(@worker_message)
    message = mq.take_next_task(@worker_message.version,:task)
    assert_equal @worker_message, message

    
    mq.write_error_for_task(message,"ERR",20)
    result = mq.take_result(message.job_id)
    assert_equal "ERR", result.error

    notask = false
    begin 
      mq.take_next_task(message.version,message.tasktype).task_id
    rescue Skynet::RequestExpiredError
      notask = true
    end
  end

  def test_write_complex_error
    assert mq.write_task(@worker_message)
    message = mq.take_next_task(@worker_message.version,:task)
    assert_equal @worker_message, message

    e = {:error => :now}
    mq.write_error_for_task(message,e,20)
    result = mq.take_result(message.job_id)
    assert_equal e, result.error
  end  
  
  ## TODO
  # def test_unmarshable_object_error
  # end
  
  # def test_raw_vs_real_task_object
  # end
  
  # def test_task_error_messages
  # end  
  
  # def expired_tasks_vs_start_after
  # end
  
  # def test_start_after
  # end
  
  # def test_multiple_jobs_and_tasks
  # end
  
  # def test_iterations
  # know when to write failover
  # end
  
  def test_worker_version
    mq.set_worker_version(2)
    assert_equal 2, mq.get_worker_version
    mq.set_worker_version(10)
    assert_equal 10, mq.get_worker_version
  end
    
end