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
    @worker_message = Skynet::WorkerStatusMessage.new(
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
   
  def test_worker_status_message
    assert_equal 13, @worker_message.to_a.size  
  end              
  
  def test_worker_status_template
    template = Skynet::WorkerStatusMessage.worker_status_template(:hostname => "localhost", :process_id => $$)
    assert_equal 13, template.size
  end
end
