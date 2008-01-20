require File.dirname(__FILE__) + '/test_helper.rb'

class SkynetTaskTest < Test::Unit::TestCase
  
  def setup
    Skynet.configure(
      :ENABLE                         => false,
      :SKYNET_LOG_FILE                => STDOUT,
      :SKYNET_LOG_LEVEL               => Logger::ERROR
    )              
  end      
  
  def test_master_task
    job = Skynet::Job.new(:map_reduce_class => self.class, :async => true)
    master_task = Skynet::Task.master_task(job)
    master_job = Skynet::Job.new(master_task.process)
    assert_equal self.class.to_s, master_job.map
    assert ! master_job.async
    assert_equal true, master_job.local_master
  end

  def self.map(datas)
  end
end