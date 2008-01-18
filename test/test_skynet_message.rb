ENV["RAILS_ENV"] = "test"

require 'test/unit'
require 'pp'        
require '../lib/skynet.rb'

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
      :version      => 1,
      :queue_id     => 0
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
      :retry        => 2,
      :queue_id     => 0
    )     
  end    
  
  def test_worker_status_message
    setup_worker_message
    {:queue_id=>0,
     :tasksubtype=>:worker,
     :tasktype=>:status,
     :name=>"name",
     :hostname=>nil,
     :worker_id=>nil,
     :map_or_reduce=>nil,
     :process_id=>nil,
     :started_at=>nil,
     :job_id=>1,
     :iteration=>0,
     :processed=>nil,
     :task_id=>2,
     :version=>1}.each do |key, val|
       assert_equal val, @worker_message.send(key)
     end
  end              

  def test_task_message
    setup_task_message
    {:payload=>"payload",
     :queue_id=>0,
     :tasktype=>:task,
     :name=>"name",
     :payload_type=>:task,
     :expiry=>20,
     :drburi=>"localhost",
     :expire_time=>0,
     :job_id=>1,
     :iteration=>0,
     :retry=>2,
     :task_id=>2,
     :version=>1}.each do |key, val|
       assert_equal val, @task_message.send(key)
     end
  end              
  

  def test_worker_status_template
    template = Skynet::WorkerStatusMessage.worker_status_template(:hostname => "localhost", :process_id => $$)
    [:status,:worker, nil, "localhost", :skip, nil, nil, nil, nil, nil, nil, nil, nil, nil].each_with_index do |field, ii|
      next if field == :skip
      assert_equal field, template[ii]
    end
  end
  
  def test_fallback_message
    setup_task_message
    fb = @task_message.fallback_task_message
    test_fb = {:payload=>"payload",
     :queue_id=>0,
     :tasktype=>:task,
     :name=>"name",
     :payload_type=>:task,
     :expiry=>20,
     :drburi=>"localhost",
     :expire_time=>1200599892,
     :job_id=>1,
     :iteration=>1,
     :retry=>2,
     :task_id=>2,
     :version=>1}
    
    test_fb.each do |key, val| 
      next if key == :expire_time
      assert_equal val, fb.send(key)
    end
    
  end
  
  def test_retry
    setup_task_message
    test_fb = {:payload=>"payload",
     :queue_id=>0,
     :tasktype=>:task,
     :name=>"name",
     :payload_type=>:task,
     :expiry=>20,
     :drburi=>"localhost",
     :job_id=>1,
     :retry=>2,
     :task_id=>2,
     :version=>1}
     
    fb1 = @task_message.fallback_task_message
    test_fb.merge(:iteration => 1).each do |k,v| 
      assert_equal v, fb1.send(k)
    end

    fb2 = fb1.fallback_task_message
    test_fb.merge(:iteration => 2).each do |k,v| 
      assert_equal v, fb2.send(k)
    end

    fb3 = fb2.fallback_task_message
    test_fb.merge(:iteration => -1).each do |k,v| 
      assert_equal v, fb3.send(k)
    end
  end
  
  def test_next_task_template
    template = Skynet::Message.next_task_template(0,:any, 99)
    [:task, nil, nil, nil, nil, :any, nil, nil, :skip, 0..6, 0, nil, 99].each_with_index do |field, ii|
      next if field == :skip
      assert_equal field, template[ii]
    end
  end      
  
  def test_result_template
    template = Skynet::Message.result_template(88,:task)
    [:task, nil, nil, 88, nil, nil, nil, nil, nil, nil, nil, nil, nil].each_with_index do |field, ii|
      assert_equal field, template[ii]
    end
  end            
  
  def test_result_message
    setup_task_message
    result = @task_message.result_message("result")
    {
       :payload=>"result",
       :queue_id=>0,
       :tasktype=>:result,
       :name=>"name",
       :payload_type=>:result,
       :expiry=>20,
       :drburi=>"localhost",
       :expire_time=>0,
       :job_id=>1,
       :iteration=>0,
       :retry=>2,
       :task_id=>2,
       :version=>1
     }.each do |key, val| 
       assert_equal val, result.send(key)
     end
     
  end
  
  def test_fallback_template
    setup_task_message
    template = @task_message.fallback_template
    [:task, "localhost", 2, nil, nil, nil, nil, nil, nil, 1..6, 1, nil, 0].each_with_index do |field, ii|
      assert_equal field, template[ii]
    end
  end
  
  def test_no_retry
    setup_task_message
    @task_message.retry = 0
    fb1 = @task_message.fallback_task_message
    {
      :payload=>"payload",
       :queue_id=>0,
       :tasktype=>:task,
       :name=>"name",
       :payload_type=>:task,
       :expiry=>20,
       :drburi=>"localhost",
       :job_id=>1,
       :iteration=>-1,
       :retry=>0,
       :task_id=>2,
       :version=>1
     }.each do |k,v| 
       assert_equal v, fb1.send(k)
     end
  end
  
  def test_payload
    setup_task_message
    @task_message.payload = "hi"
    assert_equal YAML::dump("hi"), @task_message.raw_payload
  end
  
  
end
