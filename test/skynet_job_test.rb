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
    {:map_name=>"SkynetJobTest MAP",
     :map_timeout=>60,
     :mappers=>2,
     :name=>"SkynetJobTest MASTER",
     :reducers=>1,
     :reduce_partitioner=>"SkynetJobTest",
     :reduce=>"SkynetJobTest",
     :queue_id=>0,
     :start_after=>0,
     :reduce_timeout=>60,
     :reduce_name=>"SkynetJobTest REDUCE",
     :master_timeout=>60,
     :map=>"SkynetJobTest",
     :version=>1,
     :result_timeout=>1200,
     :master_result_timeout=>1200}.each do |key, val|    
        assert_equal val, job.send(key), key
      end
   end
  
  def test_new_async_job
    job = Skynet::AsyncJob.new(:map_reduce_class => self.class)
    {:map_timeout=>60,
     :reduce_partitioner=>"SkynetJobTest",
     :master_timeout=>60,
     :reduce_timeout=>60,
     :name=>"SkynetJobTest MASTER",
     :version=>1,
     :result_timeout=>1200,
     :map_name=>"SkynetJobTest MAP",
     :reducers=>1,
     :queue_id=>0,
     :reduce_name=>"SkynetJobTest REDUCE",
     :map=>"SkynetJobTest",
     :async=>true,
     :master_result_timeout=>1200,
     :mappers=>2,
     :reduce=>"SkynetJobTest",
     :start_after=>0}.each do |key, val|    
       assert_equal val, job.send(key), key
     end
  end

  def test_master_task
    job = Skynet::AsyncJob.new(:map_reduce_class => self.class,:version=>1, :queue_id => 4)
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
    assert_equal 4, master_job.queue_id
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
    :queue_id         => 6,
    :version          => 1, 
    :map_data         => [1], 
    :master_retry     => 17,
    :master_result_timeout      => 1
    )
    job_id = nil
    Skynet.configure(:SKYNET_LOG_LEVEL => Logger::ERROR, :SOLO => false) do
      job_id = job.run
    end        
    assert job_id.is_a?(Bignum)
    test_message = {
      :version=>1,
      :queue_id=>6,
      :expire_time=>0,
      :payload_type=>:master,
      :name=>"SkynetJobTest MASTER",
      :retry=>17,
      :iteration=>0,
      :tasktype=>:task,
      :drburi=>nil,
      :expiry=>60
    }     
    message = mq.take_next_task(1,nil,nil,6)
    assert message.task_id.is_a?(Bignum)
    assert message.job_id.is_a?(Bignum)
    assert message.payload.is_a?(Skynet::Task)
    test_message.each do |k,v|
      assert_equal v, message.send(k)
    end
  end
  
  def test_run_map
    job = Skynet::AsyncJob.new(
      :map_reduce_class => self.class,
      :version          => 1, 
      :map_data         => [1], 
      :map_retry        => 2,
      :queue_id         => 7
    )
    
    Skynet.configure(:SKYNET_LOG_LEVEL => Logger::ERROR) do
      job.run_map
    end
    message = mq.take_next_task(1,nil,nil,7)
    test_message = {
      :version=>1,
      :queue_id=>7,
      :iteration=>0,
      :name=>"SkynetJobTest MAP",
      :tasktype=>:task,
      :expire_time=>0,
      :payload_type=>:task,
      :drburi=>nil,
      :expiry=>60,
      :retry=>2
     }
     assert message.task_id.is_a?(Bignum)
     assert message.job_id.is_a?(Bignum)
     assert message.payload.is_a?(Skynet::Task)
     test_message.each do |k,v|
       assert_equal v, message.send(k)
     end
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
    reduce_tasks = job.reduce_tasks([[1,2,3]])
    assert_equal 2, reduce_tasks.size
    assert_equal 9, reduce_tasks.first.retry
    assert reduce_tasks[0].task_id != reduce_tasks[1].task_id
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

    test_message = {
      :version=>1,
       :queue_id=>0,
       :iteration=>0,
       :name=>"SkynetJobTest REDUCE",
       :tasktype=>:task,
       :expire_time=>0,
       :payload_type=>:task,
       :drburi=>nil,
       :expiry=>60,
       :retry=>11      
    }

    message = mq.take_next_task(1)
    assert message.task_id.is_a?(Bignum)
    assert message.job_id.is_a?(Bignum)
    assert message.payload.is_a?(Skynet::Task)
    test_message.each do |k,v|
      assert_equal v, message.send(k)
    end
    task = message.payload
    test_task = {
      :data           => [1,3],
      :map_or_reduce  => :reduce,
      :marshalable    => true,
      :name           => "SkynetJobTest REDUCE",
      :process        => "SkynetJobTest",
      :result_timeout => 60,
      :retry          => 11
    }
    test_task.each do |k,v|
      assert_equal v, task.send(k)
    end
    assert task.task_id.is_a?(Bignum)
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
    message = messages.first.result_message(["works"])

    test_message = {
      :version      =>1,
      :queue_id     =>0,
      :iteration    =>0,
      :name         =>"hi",
      :tasktype     =>:result,
      :expire_time  =>0,
      :payload_type =>:result,
      :payload      =>["works"],
      :drburi       =>nil,
      :expiry       =>2,
      :retry        =>3
    }
    assert message.task_id.is_a?(Bignum)
    assert message.job_id.is_a?(Bignum)
    test_message.each do |k,v|
      assert_equal v, message.send(k), k
    end

    
    mq.expects(:take_result).with(job.job_id, 2).returns(message)
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
  
  def test_enqueue_messages   
    job = Skynet::Job.new(:map_data => [1,2,3], :map_reduce_class => self)
    mq = functor
    job.expects
    message = Skynet::Message.
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

  
