ENV["RAILS_ENV"] = "test"

require 'test/unit'
require 'pp'        
require '../lib/skynet.rb'


# Tests for the partitioners.
#
class MysqlMessageQueueTest < Test::Unit::TestCase
  
  def setup
    Skynet.configure(
      :ENABLE                         => false,
      :SKYNET_LOG_FILE                => STDOUT,
      :SKYNET_LOG_LEVEL               => Logger::ERROR
    )              
  end      

  def setup_worker_message
    @worker_message = Skynet::WorkerStatusMessage.new(
      :tasktype     => :task, 
      :drburi       => "localhost", 
      :job_id       => 1,
      :task_id      => 2,
      :payload      => "payload",
      :payload_type => "task",
      :expiry       => 20, 
      :expire_time  => 1095108406.9251,
      :iteration    => 0,
      :name         => "name",       
      :version      => 1
    )
  end    

  def setup_task_message
    @task_message = Skynet::Message.new(
      :tasktype     => :task, 
      :drburi       => "localhost", 
      :job_id       => 1,
      :task_id      => 2,
      :payload      => "payload",
      :payload_type => "task",
      :expiry       => 20, 
      :expire_time  => 0,
      :iteration    => 0,
      :name         => "name",       
      :version      => 1,
      :retry        => 2
    )     
  end    
  
  def test_worker_status_message
    setup_worker_message
    assert_equal 13, @worker_message.to_a.size  
  end              
  
  def test_worker_status_template
    template = Skynet::WorkerStatusMessage.worker_status_template(:hostname => "localhost", :process_id => $$)
    assert_equal 13, template.size
  end
  
  def test_fallback_message
    setup_task_message
    fb = @task_message.fallback_task_message
    assert_equal fb.iteration, 1
  end
  
  def test_retry
    setup_task_message
    fb1 = @task_message.fallback_task_message
    assert_equal 1, fb1.iteration
    fb2 = fb1.fallback_task_message
    assert_equal 2, fb2.iteration
    fb3 = fb2.fallback_task_message
    assert_equal -1, fb3.iteration
  end
  
  def test_no_retry
    setup_task_message
    @task_message.retry = 0
    fb1 = @task_message.fallback_task_message
    assert_equal -1, fb1.iteration    
  end
  
  
end
