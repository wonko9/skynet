require File.dirname(__FILE__) + '/test_helper.rb'

class SkynetJobTest < Test::Unit::TestCase

  def setup
    Skynet.configure(
    :ENABLE                => false,
    :SKYNET_LOG_FILE       => STDOUT,
    :MESSAGE_QUEUE_ADAPTER => "Skynet::MessageQueueAdapter::TupleSpace",
    :SKYNET_LOG_LEVEL      => Logger::ERROR,
    :TS_DRBURIS            => ["druby://localhost:47999"],
    :TS_USE_RINGSERVER     => false
    )      

    Skynet::MessageQueue.any_instance.stubs(:get_worker_version).returns(1)
    Skynet::MessageQueue.any_instance.stubs(:set_worker_version).returns(1)

    @ts = Rinda::TupleSpace.new
    Skynet::MessageQueueAdapter::TupleSpace.stubs(:get_tuple_space).returns(@ts)

    @messages = []
  end

  def test_new_job
    job                      = Skynet::Job.new(:map_reduce_class => self.class)
    { 
      :map_name              => "SkynetJobTest MAP",
      :map_timeout           => 60,
      :mappers               => 2,
      :name                  => "SkynetJobTest MASTER",
      :reducers              => 1,
      :reduce_partition      => nil,
      :reduce                => "SkynetJobTest",
      :queue_id              => 0,
      :start_after           => 0,
      :reduce_timeout        => 60,
      :reduce_name           => "SkynetJobTest REDUCE",
      :master_timeout        => 60,
      :map                   => "SkynetJobTest",
      :version               => 1,
      :result_timeout        => 1200,
      :master_result_timeout => 1200
    }.each do |key, val|    
      assert_equal val, job.send(key), key
    end
  end

  def test_new_async_job
    job = Skynet::AsyncJob.new(:map_reduce_class => self.class)
    {
      :map_timeout           => 60,
      :reduce_partition      => nil,
      :master_timeout        => 60,
      :reduce_timeout        => 60,
      :name                  => "SkynetJobTest MASTER",
      :version               => 1,
      :result_timeout        => 1200,
      :map_name              => "SkynetJobTest MAP",
      :reducers              => 1,
      :queue_id              => 0,
      :reduce_name           => "SkynetJobTest REDUCE",
      :map                   => "SkynetJobTest",
      :async                 => true,
      :master_result_timeout => 1200,
      :mappers               => 2,
      :reduce                => "SkynetJobTest",
      :start_after           => 0
    }.each do |key, val|    
      assert_equal val, job.send(key), key
    end
  end
  
  def test_run_async
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [[1]], 
      :mappers          => 1
    )
    job.stubs(:use_local_queue?).returns(true)
    assert_equal job.job_id, job.run
    assert_equal 1, job.local_mq.messages.size                  
    master_job = Skynet::Job.new(job.local_mq.messages.first.payload.process)
    assert ! master_job.async
    assert_equal true, master_job.local_master    
  end
  
  def test_run_master
    job = Skynet::Job.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [[1]], 
      :mappers          => 1,
      :local_master     => false
    )
    mq = functor
    received_messages = []
    mq.write_message = lambda do |message,timeout|
      received_messages << message
    end
    mq.get_worker_version = 1
    job.stubs(:mq).returns(mq)
    result_message = functor(:payload => "result", :payload_type => :result, :task_id => 1)
    mq.expects(:take_result).with(job.job_id,120).times(1).returns(result_message)
    assert_equal ["result"], job.run
    assert_equal 1, received_messages.size
  end

  def test_run
    job = Skynet::Job.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2,3], 
      :mappers          => 2,
      :reducers         => 2
    )
    job.stubs(:use_local_queue?).returns(true)
    assert_equal [[1,2],[3]], job.run.sort
  end
  
  def test_master_enqueue
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [[1]], 
      :mappers          => 1
    )
    
    mq = functor
    job.stubs(:mq).returns(mq)
    received_messages = []
    mq.write_message = lambda do |message,timeout|
      received_messages << message
    end
    mq.get_worker_version = 1

    job.master_enqueue                      
    assert_equal :master, received_messages.first.payload_type
    assert_equal :master, received_messages.first.payload.map_or_reduce
    master_job = Skynet::Job.new(received_messages.first.payload.process)
    assert_equal self.class.to_s, master_job.map
    assert ! master_job.async
    assert_equal true, master_job.local_master
  end
  
  def test_master_results
    job = Skynet::Job.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [[1]], 
      :mappers          => 1
    )
    
    mq = functor(:payload => "result")
    result_message = functor(:payload => "result", :payload_type => :result, :task_id => 1)
    mq.expects(:take_result).with(job.job_id,120).times(1).returns(result_message)
    job.stubs(:mq).returns(mq)
    results = job.master_results
    assert_equal "result", results.first
  end
  
  def test_map_enqueue
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2,3], 
      :mappers          => 2
    )
    
    mq = functor
    job.stubs(:mq).returns(mq)
    received_messages = []
    mq.write_message = lambda do |message,timeout|
      received_messages << message
    end
    mq.get_worker_version = 1

    job.map_enqueue                      
    assert_equal 2, received_messages.size
    assert_equal :task, received_messages.first.payload_type
    assert_equal :map, received_messages.first.payload.map_or_reduce
  end
  
  def test_map_results
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [[1]], 
      :mappers          => 1
    )
    
    mq = functor(:payload => "result")
    i = 0
    result_message = functor(:payload => "result", :payload_type => :result, :task_id => lambda {i += 1})
    mq.expects(:take_result).with(job.job_id,120).times(2).returns(result_message)
    job.stubs(:mq).returns(mq)
    results = job.map_results(2)
    assert_equal "result", results.first
  end

  def test_local_map_results
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2], 
      :mappers          => 2,
      :single           => true
    )
    
    job.map_enqueue      
    assert_equal 2, job.local_mq.messages.size
    results = job.map_results(2)
    assert_equal [[1],[2]], results.sort
  end

  def test_partition_data
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [[1]], 
      :reducers         => 2
    )                       
    
    partitioned_data = job.partition_data([[1],[2],[3]])
    assert_equal [[1, 3], [2]], partitioned_data
  end

  def test_partition_data_class
    job = Skynet::AsyncJob.new(
      :map_reduce_class => "SkynetJobTest::PartitionTest",    
      :version          => 1, 
      :map_data         => [[1]], 
      :reducers         => 2
    )                       
    
    partitioned_data = job.partition_data([[1],[2],[3]])
    assert_equal [[1], [2], [3]], partitioned_data
  end

  def test_reduce_enqueue
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2,3], 
      :mappers          => 2
    )
    
    mq = functor
    job.stubs(:mq).returns(mq)
    received_messages = []
    mq.write_message = lambda do |message,timeout|
      received_messages << message
    end
    mq.get_worker_version = 1

    job.reduce_enqueue([[1, 3], [2]])                      
    assert_equal 2, received_messages.size
    assert_equal :task, received_messages.first.payload_type
    assert_equal :reduce, received_messages.first.payload.map_or_reduce
  end
  
  def test_reduce_results
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [[1]], 
      :mappers          => 1
    )
    
    mq = functor(:payload => "result")
    i = 0
    result_message = functor(:payload => "result", :payload_type => :result, :task_id => lambda {i += 1})
    mq.expects(:take_result).with(job.job_id,120).times(2).returns(result_message)
    job.stubs(:mq).returns(mq)
    results = job.reduce_results(2)
    assert_equal "result", results.first
  end

  def test_master_task
    job = Skynet::AsyncJob.new(:map_reduce_class => self.class,:version=>1, :queue_id => 4)
    mt = job.master_task
    assert mt.is_a?(Skynet::Task)
    assert_equal mt.result_timeout, 60
    master_job = Skynet::Job.new(mt.process)
    assert_equal job.map, self.class.to_s
    assert_equal job.reduce, self.class.to_s
    assert_equal nil, job.reduce_partition
    assert_equal master_job.map, self.class.to_s
    assert_equal master_job.reduce, self.class.to_s
    assert_equal nil, master_job.reduce_partition
    assert_equal 4, master_job.queue_id
    Skynet::Job::FIELDS.each do |field|
      case field
      when :async, :local_master
        nil
      when :job_id, :single
        next  
      else
        assert_equal job.send(field), master_job.send(field), "Testing #{field}, jobfield: #{job.send(field)} mjobfield: #{master_job.send(field)}"
      end
    end
  end

  def test_gather_results_with_errors
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1], 
      :mappers          => 1
    )
    map_tasks = job.map_tasks
    mq = mq        
    mq.stubs(:get_worker_version).returns(1)
    job.stubs(:mq).returns(mq)                 
    messages = job.tasks_to_messages(map_tasks)
    message = messages.first.result_message(["works"])
    message2 = Skynet::Message.new(message.to_h.merge(:payload_type => :error, :payload => "error", :task_id => 33))

    test_message1 = {
      :version      =>1,
      :queue_id     =>0,
      :iteration    =>0,
      :name         =>"SkynetJobTest MAP",
      :tasktype     =>:result,
      :expire_time  =>0,
      :payload_type =>:result,
      :payload      =>["works"],
      :drburi       =>nil,
      :expiry       =>60,
      :retry        =>3
    }
    assert message.task_id.is_a?(Bignum)
    assert message.job_id.is_a?(Bignum)
    test_message1.each do |k,v|
      assert_equal v, message.send(k), k
    end

    mq.expects(:take_result).with(job.job_id, 120).returns(message,message2).times(2)
    results = nil
    Skynet.silent do
      results = job.gather_results(2, map_tasks.first.result_timeout, map_tasks.first.name)
    end
    assert_equal [["works"]], results    
  end      

  def test_map_tasks
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2,3], 
      :map_retry        => 7,
      :mappers          => 2
    )
    map_tasks = job.map_tasks
    assert_equal 2, map_tasks.size
    assert_equal 7, map_tasks.first.retry
    assert map_tasks[0].task_id != map_tasks[1].task_id
  end

  def test_reduce_tasks
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2,3], 
      :reduce_retry     => 9,
      :reducers         => 2
    )                                        
    reduce_tasks = job.reduce_tasks([1,2,3])
    assert_equal 3, reduce_tasks.size
    assert_equal 9, reduce_tasks.first.retry
    assert reduce_tasks[0].task_id != reduce_tasks[1].task_id
  end

  def test_enqueue_messages   
    passed_messages = []
    mq = functor                
    mq.write_message = lambda do |m,timeout|
      passed_messages << m      
    end
    job = Skynet::Job.new(:map_data => [1,2,3], :map_reduce_class => self.class)
    job.stubs(:mq).returns(mq)
    message1 = Skynet::Message.new(
      :tasktype     => :task, 
      :task_id      => 9, 
      :job_id       => 8, 
      :payload_type => :task, 
      :payload      => "blah",
      :retry        => 3,
      :iteration    => 0,      
      :version      => 1,
      :queue_id     => 4,
      :name         => "test" 
    )
    message2 = Skynet::Message.new(message1.to_h.merge(:name => "test2", :payload => "test2"))
    job.enqueue_messages([message1,message2])
    assert_equal [message1, message2], passed_messages
  end


  def test_local_queue_write_message
    local_queue = Skynet::Job::LocalMessageQueue.new
    assert_equal 1, local_queue.get_worker_version
    local_queue.write_message(Skynet::Message.new({}),2)
    assert local_queue.messages.first.is_a?(Skynet::Message)
  end

  def test_local_queue_take_result
    job = Skynet::AsyncJob.new(
      :map_reduce_class      => self.class,
      :queue_id              => 6,
      :version               => 1, 
      :map_data              => [1,2,3], 
      :mappers               => 2,
      :master_retry          => 17,
      :master_result_timeout => 1
    )
    tasks = job.map_tasks
    assert_equal 2, tasks.size
    messages = job.tasks_to_messages(tasks)

    local_queue = Skynet::Job::LocalMessageQueue.new
    messages.each do |message|    
      local_queue.write_message(message)
    end
    assert_equal messages, local_queue.messages
    assert_equal [1,3], local_queue.take_result(job.job_id,2).payload
    assert_equal [2], local_queue.take_result(job.job_id,2).payload
    assert_equal 0, local_queue.messages.size
    assert_equal 0, local_queue.results.size
  end

  def test_run_tasks_locally_errors    
    job = Skynet::AsyncJob.new(
      :map_reduce_class      => self.class,
      :queue_id              => 6,
      :version               => 1, 
      :map_data              => [1,2,3], 
      :mappers               => 2,
      :master_retry          => 17,
      :master_result_timeout => 1
    )
    tasks = job.map_tasks
    assert_equal 2, tasks.size
    
    messages = job.tasks_to_messages(tasks)

    task1 = messages.first.payload
    task1.extend(Functor)
    tries = 0
    task1.define_method(:tries) {@tries}
    task1.define_method(:run) do
      @tries ||= 0
      @tries += 1
      if @tries == 1
        raise Exception
      else
        return task1.data
      end      
    end  
    messages.first.expects(:payload).returns(task1).times(2)
    
    task2 = messages[1].payload
    task2.extend(Functor)
    tries = 0
    task2.define_method(:tries) {@tries}
    task2.define_method(:run) do
      @tries ||= 0
      @tries += 1
      return task2.data
    end  
    messages[1].expects(:payload).returns(task2).times(1)    

    local_queue = Skynet::Job::LocalMessageQueue.new
    messages.each do |message|    
      local_queue.write_message(message)
    end

    assert_equal messages, local_queue.messages
    Skynet.configure(:SKYNET_LOG_LEVEL=>Logger::FATAL) do
      assert_equal [1,3], local_queue.take_result(job.job_id,2).payload
      assert_equal [2], local_queue.take_result(job.job_id,2).payload
    end
    assert_equal 2, task1.tries
    assert_equal 1, task2.tries
    assert_equal 0, local_queue.messages.size
    assert_equal 0, local_queue.results.size
  end                       
  
  def test_map_local
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2,3,4],
      :mappers          => 3
    )
    
    assert_equal 3, job.map_tasks.size
    assert !job.map_local?

    job.keep_map_tasks = true
    assert job.map_local?

    job.keep_map_tasks = 3
    assert job.map_local?

    job.keep_map_tasks = 2
    assert !job.map_local?
    
    job.single = true
    assert job.map_local?
  end                    

  def test_reduce_local
    job = Skynet::AsyncJob.new(
      :reduce_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2,3,4],
      :reducers         => 3
    )
                                                     
    partitioned_data = [[1,2],[3],[4]]
    assert_equal 3, job.reduce_tasks(partitioned_data).size
    assert !job.reduce_local?(job.reduce_tasks(partitioned_data))

    job.keep_reduce_tasks = true
    assert job.reduce_local?(job.reduce_tasks(partitioned_data))

    job.keep_reduce_tasks = 3
    assert job.reduce_local?(job.reduce_tasks(partitioned_data))

    job.keep_reduce_tasks = 2
    assert !job.reduce_local?(job.reduce_tasks(partitioned_data))
    
    job.single = true
    assert job.reduce_local?(job.reduce_tasks(partitioned_data))
  end
  


  def test_keep_map_tasks
    job = Skynet::Job.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2], 
      :mappers          => 2,
      :keep_map_tasks   => 2
    )                
    assert_equal 2, job.map_enqueue
    assert_equal 2, job.local_mq.messages.size
    assert_equal [[1],[2]], job.map_results(2).sort
    assert_equal 0, job.local_mq.messages.size
    assert_equal 0, job.local_mq.results.size
  end
  

  def test_keep_reduce_tasks
    job = Skynet::Job.new(
      :map_reduce_class  => self.class,    
      :version           => 1, 
      :map_data          => [1,2], 
      :reducers          => 1,
      :keep_reduce_tasks => 2
    )                
    assert_equal 1, job.reduce_enqueue([[1,2]])
    assert_equal 1, job.local_mq.messages.size
    assert_equal [[1,2]], job.reduce_results(1).sort
    assert_equal 0, job.local_mq.messages.size
    assert_equal 0, job.local_mq.results.size    
  end
  
  
  def self.map(datas)        
    ret = []
    datas.each do |data|
      if data == :error
        raise Exception.new("something bad happened")
      else 
        ret << data
      end
    end
    return ret
  end                

  def self.reduce(datas)
    datas
  end

  private

  def mq
    Skynet::MessageQueueAdapter::TupleSpace.new
  end

  class PartitionTest < SkynetJobTest
    
    def self.reduce_partition(post_map_data, reducers)
      return post_map_data.compact
    end
  end

end


