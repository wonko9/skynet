require File.dirname(__FILE__) + '/test_helper.rb'

class MysqlMessageQueueTest < Test::Unit::TestCase
  ENV["RAILS_ENV"] = "test"
  
  ActiveRecord::Base.establish_connection(
    :adapter  => "mysql",
    :host     => "localhost",
    :username => "root",
    :password => "",
    :database => "skynet_test"
  )
  ActiveRecord::Base.connection.execute("delete from skynet_message_queues")
  ActiveRecord::Base.connection.execute("delete from skynet_worker_queues")
  
  def setup
    Skynet.configure(
      :ENABLE                         => false,
      # :SKYNET_PIDS_FILE               => File.expand_path("#{RAILS_ROOT}/log/skynet_#{RAILS_ENV}.pids"),
      :SKYNET_LOG_FILE                => STDOUT,
      :SKYNET_LOG_LEVEL               => Logger::ERROR,
      :MESSAGE_QUEUE_ADAPTER          => "Skynet::MessageQueueAdapter::Mysql",
      :MYSQL_NEXT_TASK_TIMEOUT        => 1,
      :MYSQL_TEMPERATURE_CHANGE_SLEEP => 1
    )      

    ActiveRecord::Base.establish_connection(
      :adapter  => "mysql",
      :host     => "localhost",
      :username => "root",
      :password => "",
      :database => "skynet_test"
    )
    ActiveRecord::Base.connection.execute("delete from skynet_message_queues")
    ActiveRecord::Base.connection.execute("delete from skynet_worker_queues")
    
    @message_options = {
      :tasktype     => "task", 
      :job_id       => 1,
      :task_id      => 2,
      :payload      => "payload",
      :payload_type => "task",
      :expiry       => 20, 
      :expire_time  => 1095108406.9251,
      :iteration    => 0,
      :name         => "name",       
      :version      => 1,
      :queue_id     => 0,
      :retry        => 1
    }
    @worker_message = Skynet::Message.new(@message_options)
    
  end
                   
### THE FOLLOWING TESTS THE MessageQueueProxy API
  
  def test_write_message    
    mq.write_message(@worker_message,10)
    message = SkynetMessageQueue.find(:first, :conditions => "tasktype = 'task'")
    assert_equal @worker_message.expiry.to_f, 20          
    @message_options.each do |key,val|
      next if key == :payload
      assert_equal message.send(key), val
    end    
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
    message = SkynetMessageQueue.find(:first, :conditions => "tasktype = 'task'")
    assert_equal 0, message.iteration
    task = mq.take_next_task(1,1,:task)                       
    assert_equal @worker_message.payload, task.payload

    message = SkynetMessageQueue.find(:first, :conditions => "tasktype = 'task'")
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
    ntt = Skynet::Message.next_task_template(1)
    next_task = mq.take_next_task(1,1)
    assert_equal 2, next_task.task_id
    assert_equal 1, next_task.iteration
  end               
  
  def test_take_race_condition
    message = @worker_message.clone
    message.expiry=0.4
    assert mq.write_message(message)
    template = Skynet::Message.next_task_template(1, :task, 0)

    message_row = mq.send(:find_next_message,template,:task)
    assert !message_row.tran_id

    assert_equal 1, mq.send(:write_fallback_message,message_row, message)
    assert_equal 0, mq.send(:write_fallback_message,message_row, message)
    assert_equal 0, mq.send(:write_fallback_message,message_row, message)

    template = Skynet::Message.next_task_template(1, :task, 0)
    another_row = mq.send(:find_next_message,template,:task)
    assert !another_row
    sleep 1
    template = Skynet::Message.next_task_template(1, :task, 0)
    message_row2 = mq.send(:find_next_message,template,:task)
    assert message_row2
    assert_equal message_row.tran_id, message_row2.tran_id

    message_row2.tran_id = 4
    assert_equal 0, mq.send(:write_fallback_message,message_row2, message)
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
    assert_equal 1, SkynetMessageQueue.count(:id, :conditions => "tasktype = 'version'")
  end
  
  def test_version_active?
    mq.set_worker_version(98)
    @worker_message.version = mq.get_worker_version
    mq.write_message(@worker_message,10)
    message = SkynetMessageQueue.find(:first, :conditions => "tasktype = 'task'")
    assert_equal message.version, 98
    mq.set_worker_version(99)
    assert_equal 99, mq.get_worker_version
    assert_equal true, mq.version_active?(98)
    assert_equal true, mq.version_active?(99)
    message.destroy
    assert_equal false, mq.version_active?(98)    
    assert_equal true, mq.version_active?(99)    
  end
    

  private

  def mq
		Skynet::MessageQueueAdapter::Mysql.new
  end

end