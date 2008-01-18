ENV["RAILS_ENV"] = "test"

require 'test/unit'
require 'pp'        
require '../lib/skynet.rb'
require 'rubygems'
require 'mocha'

class SkynetManagerTest < Test::Unit::TestCase

  PIDS = []
  PIDFILE = []
  
  def setup
    Skynet.configure(
      :ENABLE                         => false,
      # :SKYNET_PIDS_FILE               => File.expand_path("#{RAILS_ROOT}/log/skynet_#{RAILS_ENV}.pids"),
      :SKYNET_LOG_FILE                => STDOUT,
      :SKYNET_LOG_LEVEL               => Logger::ERROR,
      :MESSAGE_QUEUE_ADAPTER          => "Skynet::MessageQueueAdapter::Mysql",
      :MYSQL_NEXT_TASK_TIMEOUT              => 1
    )      
    ActiveRecord::Base.establish_connection(
        :adapter  => "mysql",
        :host     => "localhost",
        :username => "root",
        :password => "",
        :database => "skynet_test"
      )

    mq.clear_outstanding_tasks
    mq.clear_worker_status

    @pids = []
    @pidfile = []
    
    PIDS.delete_if {true}
    PIDFILE.delete_if {true}

    Process.stubs(:detatch).returns(true)    
    Process.stubs(:kill).returns(true)

    @minstance = Skynet::Manager.any_instance
   
    Skynet::Worker.any_instance.stubs(:max_memory_reached?).returns(false)

    @manager = Skynet::Manager.new(:script_path => "path", :workers => 2)
    
    def @manager.fork
      newpid = SkynetManagerTest::PIDS.size + 1
      SkynetManagerTest::PIDS << newpid
      worker = Skynet::Worker.new(:any,{:process_id => newpid})
      worker.stubs(:process_id).returns(newpid)
      worker.notify_worker_started
      newpid
    end
  end  
  
  def test_clear_worker_status  
    @manager.start_workers
    assert_equal 2, @manager.worker_pids.size
    assert_equal PIDS.sort, @manager.worker_pids.sort
    assert_equal PIDS.sort, @manager.worker_queue.collect {|q|q.process_id}.sort
    assert_equal 2, SkynetWorkerQueue.count(:id, :conditions => "id is not null")
    mq.clear_worker_status
    assert_equal 0, SkynetWorkerQueue.count(:id, :conditions => "id is not null")
    
  end

  def test_manager_start  
    @manager.start_workers
    assert_equal 2, @manager.worker_pids.size
    assert_equal PIDS.sort, @manager.worker_pids.sort
    assert_equal PIDS.sort, @manager.worker_queue.collect {|q|q.process_id}.sort
  end

  def test_check_workers
    Skynet::Manager.any_instance.expects(:worker_alive?).times(2).returns(true)
    @manager.start_workers
    @manager.check_workers
    assert_equal 2, @manager.worker_pids.size
    assert_equal PIDS.sort, @manager.worker_pids.sort
    assert_equal PIDS.sort, @manager.worker_queue.collect {|q|q.process_id}.sort
  end
  
  def test_running_pids
    Skynet::Manager.any_instance.expects(:worker_alive?).with(1).returns(true)
    Skynet::Manager.any_instance.expects(:worker_alive?).with(2).returns(false)
    @manager.start_workers
    @manager.check_workers
    assert_equal 1, @manager.worker_pids.size
    assert_equal [1], @manager.worker_pids.sort
    assert_equal [1], @manager.worker_queue.collect {|q|q.process_id}.sort
  end

  ## XXX FIXME.  What happens if there's a worker missing from the pidfile, but was running?
  def test_more_in_pidfile_than_queue_alive
    Skynet::Manager.any_instance.expects(:worker_alive?).with(1).returns(true)
    Skynet::Manager.any_instance.expects(:worker_alive?).with(2).returns(true)
    @manager.start_workers
    SkynetWorkerQueue.find(:first, :conditions => "process_id = 1").destroy
    @manager.check_workers
    assert_equal 1, @manager.worker_pids.size
    assert_equal [2], @manager.worker_pids.sort
    assert_equal [2], @manager.worker_queue.collect {|q|q.process_id}.sort
  end

  def test_dead_workers
    Skynet::Manager.any_instance.expects(:worker_alive?).times(1).with(1).returns(false)
    # Skynet::Manager.any_instance.expects(:worker_alive?).with(2).returns(true)
    @manager.start_workers     
    @manager.check_workers
    assert_equal [2], @manager.worker_pids.sort
    assert_equal [2], @manager.worker_queue.collect {|q|q.process_id}.sort
  end


  private

  def mq
		Skynet::MessageQueueAdapter::Mysql.new
  end
end
