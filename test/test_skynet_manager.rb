require File.dirname(__FILE__) + '/test_helper.rb'
require 'functor'

class SkynetManagerTest < Test::Unit::TestCase

  PIDS = []
  PIDFILE = []

  def setup
    connection = ActiveRecord::Base.establish_connection(
        :adapter  => "mysql",
        :host     => "localhost",
        :username => "root",
        :password => "",
        :database => "skynet_test"
      )

    ActiveRecord::Base.connection.reconnect!

    Skynet.configure(
      :ENABLE                         => false,
      # :SKYNET_PIDS_FILE               => File.expand_path("#{RAILS_ROOT}/log/skynet_#{RAILS_ENV}.pids"),
      :SKYNET_LOG_FILE                => STDOUT,
      :SKYNET_LOG_LEVEL               => 4,
      :MESSAGE_QUEUE_ADAPTER          => "Skynet::MessageQueueAdapter::Mysql",
      :MYSQL_TEMPERATURE_CHANGE_SLEEP => 1,
      :MYSQL_NEXT_TASK_TIMEOUT        => 1,
      :MYSQL_QUEUE_DATABASE           => nil
    )

    mq.clear_outstanding_tasks
    mq.clear_worker_status

    @pids = []
    @pidfile = []
    @worker_queue = []

    PIDS.delete_if {true}
    PIDFILE.delete_if {true}

    Process.stubs(:detatch).returns(true)
    Process.stubs(:kill).returns(true)

    @minstance = Skynet::Manager.any_instance
    @hostname = Socket.gethostname

    Skynet::Worker.any_instance.stubs(:max_memory_reached?).returns(false)
  end

  def setup_manager
    @manager = Skynet::Manager.new(:script_path => "path", :workers => 2)
    @manager.extend(Functor)

    mq = functor
    @manager.stubs(:mq).returns(mq)

    mq.get_worker_version = 1

    worker_queue = @worker_queue
    mq.read_all_worker_statuses = lambda do |hostname|
      worker_queue
    end

    mq.take_worker_status = lambda do |options, timeout|
      worker_queue.delete_if {|w|w.process_id == options[:process_id] }.first
    end

    pids     = @pids
    hostname = @hostname
    Skynet.extend(Functor)
    Skynet.define_method(:close_files) do
      true
    end

    Skynet.define_method(:close_console) do
      true
    end


    Skynet.define_method(:fork_and_exec) do |cmd|
      newpid = pids.size + 1
      pids << newpid
      worker_info = {
        :hostname     => hostname,
        :process_id   => newpid,
        :worker_type  => :task,
        :worker_id    => newpid,
        :version      => 1
      }

      worker_queue << Skynet::WorkerStatusMessage.new(worker_info)
      newpid
    end
  end

  def test_manager_start
    setup_manager
    @manager.start_workers
    assert_equal 2, @manager.worker_pids.size
    assert_equal @pids.sort, @manager.worker_pids.sort
    assert_equal @pids.sort, @manager.worker_queue.collect {|q|q.process_id}.sort
  end

  def test_check_workers
    setup_manager
    Skynet::Manager.any_instance.expects(:worker_alive?).times(2).returns(true)
    @manager.start_workers
    @manager.check_workers
    assert_equal 2, @manager.worker_pids.size
    assert_equal @pids.sort, @manager.worker_pids.sort
    assert_equal @pids.sort, @manager.worker_queue.collect {|q|q.process_id}.sort
  end

  def test_running_pids
    setup_manager
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
    setup_manager
    @manager.expects(:worker_alive?).with(1).returns(false)
    Skynet.configure(:SKYNET_LOG_LEVEL => 4) do
      @manager.start_workers

      @manager.expects(:worker_alive?).with(2).returns(true)
      @worker_queue.delete_if {|w| w.process_id == 1}
      @manager.check_workers
    end
    assert_equal 2, @manager.worker_pids.size
    assert_equal [2,3], @manager.worker_pids.sort
    assert_equal [2,3], @manager.worker_queue.collect {|q|q.process_id}.sort
  end

  def test_dead_workers
    setup_manager
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
