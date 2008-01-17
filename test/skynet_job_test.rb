require 'test/unit'
require 'pp'        
require '../lib/skynet.rb'
require 'rubygems'
require 'mocha'
require 'functor'

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
    job = Skynet::Job.new(:map_reduce_class => self.class)
    assert_equal job.master_timeout, 60
  end
  
  def test_new_async_job
    job = Skynet::AsyncJob.new(:map_reduce_class => self.class)
    assert_equal job.master_timeout, 60
  end

  def test_master_task
    job = Skynet::AsyncJob.new(:map_reduce_class => self.class,:version=>1)
    mt = job.master_task
    assert mt.is_a?(Skynet::Task)
    assert_equal mt.result_timeout, 60
    master_job = Skynet::Job.new(mt.process)
    assert_equal job.map, self.class.to_s
    assert_equal job.reduce, self.class.to_s
    assert_equal job.reduce_partitioner, self.class.to_s
    assert_equal master_job.map, self.class.to_s
    assert_equal master_job.reduce, self.class.to_s
    assert_equal master_job.reduce_partitioner, self.class.to_s
    Skynet::Job::FIELDS.each do |field|
      case field
      when :async
        nil
      when :job_id, :single
        next  
      else
        assert_equal job.send(field), master_job.send(field), "Testing #{field}, jobfield: #{job.send(field)} mjobfield: #{master_job.send(field)}"
      end
    end
  end
  
  def test_run
    job = Skynet::AsyncJob.new(
    :map_reduce_class => self.class,
    :version          => 1, 
    :map_data         => [1], 
    :master_retry     => 17
    )

    Skynet.configure(:SKYNET_LOG_LEVEL => Logger::ERROR) do
      job.run
    end
    message = mq.take_next_task(1)
    assert_equal message.payload_type, :master
    assert_equal message.retry, 17
    assert_equal 17, message.payload.retry    
  end
  
  def test_run_map
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,
      :version          => 1, 
      :map_data         => [1], 
      :map_retry        => 2
    )
    
    Skynet.configure(:SKYNET_LOG_LEVEL => Logger::ERROR) do
      job.run_map
    end
    message = mq.take_next_task(1)
    assert_equal message.payload_type, :task
    assert_equal message.retry, 2        
    assert_equal 2, message.payload.retry    
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
  end

  def test_reduce_tasks
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2,3], 
      :reduce_retry     => 9,
      :reducers         => 2
    )                                        
    reduce_tasks = job.reduce_tasks([[1,2,3]])
    assert_equal 2, reduce_tasks.size
    assert_equal 9, reduce_tasks.first.retry
  end

  def test_run_reduce
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2,3], 
      :reduce_retry     => 11,
      :reducers         => 2
    )                                        
    Skynet.configure(:SKYNET_LOG_LEVEL => Logger::ERROR) do
      job.run_reduce([[1,2,3]])
    end

    message = mq.take_next_task(1)
    assert_equal message.payload_type, :task
    assert_equal message.retry, 11
    task = message.payload
    assert_equal 11, task.retry      
  end                          
  
  def test_gather_results
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1], 
      :mappers          => 1
    )
    map_tasks = job.map_tasks
    mq = mq
    job.stubs(:mq).returns(mq)                 
    messages = job.messages_from_tasks(map_tasks, 2, "hi")
    result_message = messages.first.result_message(["works"])
    
    mq.expects(:take_result).with(job.job_id, 2).returns(result_message)
    results = job.gather_results(map_tasks,1,"hi")
    assert_equal [["works"]], results    
  end
  
  def test_run_messages_locally
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [[1]], 
      :mappers          => 1
    )
    messages = job.messages_from_tasks(job.map_tasks, 1, "hi")
    results = job.run_messages_locally(messages)
    assert_equal [[[1]]], results    
  end                          
  
  def test_run_messages_locally_errors
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,
      :version          => 1, 
      :map_data         => [[9]], 
      :map_retry        => 1,
      :mappers          => 1
    )                                       
    tasks = job.map_tasks  

    messages = job.messages_from_tasks(tasks, 1, "hi")
    tries = 0
    task = messages.first.payload
    task.extend(Functor)
    task.define_method(:run) do
      tries += 1
      if tries == 1
        raise Exception
      else
        return [1]
      end      
    end  
    errors = nil
    results = job.run_messages_locally(messages)
    assert_equal 2, tries
    assert_equal [[1]], results
  end    
  
  def test_keep_map_tasks
    job = Skynet::Job.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2], 
      :mappers          => 2,
      :reducers         => 0,
      :keep_map_tasks   => 3
    )                
    map_tasks = job.map_tasks
    assert_equal 2, map_tasks.size
    job.expects(:run_messages_locally).times(1).returns([])
    job.run
  end

  def test_keep_reduce_tasks
    job = Skynet::Job.new(
      :map_reduce_class => self.class,    
      :version          => 1, 
      :map_data         => [1,2], 
      :mappers          => 2,
      :reducers         => 1,
      :keep_map_tasks   => true,
      :keep_reduce_tasks => 3
    )                
    map_tasks = job.map_tasks
    assert_equal 2, map_tasks.size
    job.expects(:run_messages_locally).times(2).returns([1,2])
    job.run
  end

  def self.map(datas)        
    ret = []
    datas.each do |data|
      if data.first == :error
        raise Exception.new("something bad happened")
      else 
        ret << data
      end
      return ret
    end
  end                

  def self.reduce(datas)
  end

  def self.reduce_partitioner(post_map_data,num_reducers)
    Skynet::Partitioner::recombine_and_split.call(post_map_data, num_reducers) 
  end

  private

  def mq
		Skynet::MessageQueueAdapter::TupleSpace.new
  end
  
end

  
