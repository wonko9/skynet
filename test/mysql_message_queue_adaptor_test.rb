ENV["RAILS_ENV"] = "test"

require 'test/unit'
require '../lib/skynet.rb'

class MysqlMessageQueueTest < Test::Unit::TestCase
  
  def setup
    Skynet.configure(
      :ENABLE                         => false,
      # :SKYNET_PIDS_FILE               => File.expand_path("#{RAILS_ROOT}/log/skynet_#{RAILS_ENV}.pids"),
      :SKYNET_LOG_FILE                => STDOUT,
      :SKYNET_LOG_LEVEL               => Logger::ERROR,
      :MESSAGE_QUEUE_ADAPTER          => "Skynet::MessageQueueAdapter::Mysql",
      :NEXT_TASK_TIMEOUT              => 1
    )      
    mq.clear_outstanding_tasks
    mq.clear_worker_status
    
    @worker_message = Skynet::Message.new(
      :tasktype=> :task, 
      :drburi=> "localhost", 
      :job_id => 1,
      :task_id => 2,
      :payload => "payload",
      :payload_type => "task",
      :expiry => 20, 
      :expire_time => 1095108406.9251,
      :iteration => 0,
      :name => "name",       
      :version => 1
    )
    
  end
                   
### THE FOLLOWING TESTS THE MessageQueueProxy API
  
  def test_write_message    
    mq.write_message(@worker_message,10)
    message = SkynetMessageQueue.find(:all).first
    assert_equal @worker_message.expiry.to_f, 20
  end
                                                                 
  def test_template_to_conditions
    conditions = mq.template_to_conditions(Skynet::Message.next_task_template(1,:task))
    assert_match "iteration BETWEEN 0 AND 6", conditions
    assert_match "version = 1", conditions
    assert_match "tasktype = 'task'", conditions
    assert_match "expire_time BETWEEN 0 AND", conditions
    assert_match "payload_type = 'task'", conditions
  end

  def test_message_to_conditions
    conditions = mq.template_to_conditions(Skynet::Message.new(Skynet::Message.next_task_template(1)))
    assert_match "iteration BETWEEN 0 AND 6", conditions
    assert_match "version = 1", conditions
    assert_match "tasktype = 'task'", conditions
    assert_match "expire_time BETWEEN 0 AND", conditions
  end
  
  def test_take_next_task
    assert mq.write_message(@worker_message,10)                                   
    task = mq.take_next_task(1,1,:task)                       
    assert_equal @worker_message.payload, task.payload
    
    message = SkynetMessageQueue.find(:first)
    assert_equal 1, message.iteration
    assert @worker_message.expire_time < message.expire_time            
    excep = nil
    begin 
      mq.take_next_task(1)
    rescue Skynet::RequestExpiredError
      excep = true
    end
    assert excep    
  end
  
  def test_task_failover
    message = @worker_message.clone
    message.expiry=0.4
    assert mq.write_message(message)
    task = mq.take_next_task(1)
    assert_equal 2, task.task_id
    assert_equal 0, task.iteration
    sleep 0.6
    ntt = Skynet::Message.next_task_template(1)
    next_task = mq.take_next_task(1,0.00001)
    assert_equal 2, next_task.task_id
    assert_equal 1, next_task.iteration
  end

  def test_write_and_take_result
    assert mq.write_message(@worker_message,10)  
    message = mq.take_next_task(1)
    set_result = {:blah => ['hi']}
    
    mq.write_result(message,set_result,20)    

    assert mq.list_tasks.empty?
    assert_equal 1, mq.list_results.size
    result = mq.take_result(1)
    assert_equal set_result, result.payload
  end
  
  def test_write_error
    mq.write_message(@worker_message,10)  
    message = mq.take_next_task(1)
    mq.write_error(message,"something_bad_happened",10)
    result = mq.take_result(1)          
    assert_equal "something_bad_happened", result.payload
  end

  def test_write_complex_error
    mq.write_message(@worker_message,10)  
    message = mq.take_next_task(1)
    error = {:error => "something_bad_happened"}
    mq.write_error(message,error,10)
    result = mq.take_result(1)          
    assert_equal error, result.payload
  end                      
  
  def test_write_worker_status
    assert mq.write_worker_status({
      :worker_id  => 5,
      :hostname   => 'localhost',
      :process_id => $$,
      :name       => "waiting for master or tasks",
      :iteration  => 0,
      :processed  => 0,
      :version    => 1,
      :started_at => Time.now.to_i
    })                 
    ws = SkynetWorkerQueue.find_by_worker_id(5)
    assert_equal ws.iteration, 0

    assert mq.write_worker_status({
      :worker_id  => 5,
      :hostname   => 'localhost',
      :process_id => $$,
      :name       => "waiting for master or tasks",
      :processed  => 0,
      :version    => 1,
      :started_at => Time.now.to_i
    })                            
    ws.reload
    assert_equal ws.iteration, nil    
  end
  
  def test_take_worker_status
    assert mq.write_worker_status({
      :worker_id  => 5,
      :hostname   => 'localhost',
      :process_id => $$,
      :name       => "waiting for master or tasks",
      :processed  => 0,
      :version    => 1,
      :started_at => Time.now.to_i
    })        
    
    status = mq.take_worker_status({:hostname   => 'localhost',:process_id => $$ })
    assert_equal $$, status.process_id        
  end
  
  def test_read_all_worker_statuses
    assert mq.write_worker_status({
      :worker_id  => 5,
      :hostname   => 'localhost',
      :process_id => $$,
      :name       => "waiting for master or tasks",
      :processed  => 0,
      :version    => 1,
      :started_at => Time.now.to_i
    })        
    status = mq.read_all_worker_statuses.first

    # status = mq.take_worker_status({:hostname   => 'localhost',:process_id => $$ })
    assert_equal $$, status.process_id        
  end

  def test_worker_version
    mq.set_worker_version(2)
    assert_equal 2, mq.get_worker_version
    mq.set_worker_version(10)
    assert_equal 10, mq.get_worker_version
    mq.set_worker_version(11)
    mq.set_worker_version(12)
    assert_equal 1, SkynetWorkerQueue.count(:id, :conditions => "tasktype = 'workerversion'")
  end

  private

  def mq
		Skynet::MessageQueueAdapter::Mysql.new
  end

end