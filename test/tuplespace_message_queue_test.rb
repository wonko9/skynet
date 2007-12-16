ENV["RAILS_ENV"] = "test"

require 'test/unit'
require 'pp'        
require '../lib/skynet.rb'


# Tests for the partitioners.
#
class TuplespaceMessageQueueTest < Test::Unit::TestCase
  
  def setup
    Skynet.configure(
      :ENABLE                         => false,
      # :SKYNET_PIDS_FILE               => File.expand_path("#{RAILS_ROOT}/log/skynet_#{RAILS_ENV}.pids"),
      :SKYNET_LOG_FILE                => STDOUT,
      :SKYNET_LOG_LEVEL               => Logger::ERROR,
      :SKYNET_LOCAL_MANAGER_URL       => "druby://localhost:40000",
      :MESSAGE_QUEUE_ADAPTER          => "Skynet::MessageQueueAdapter::TupleSpace",
      :TUPLESPACE_DRBURIS              => ["druby://localhost:77647"],
      :USE_RINGSERVER                 => false
    )      
    @ts ||= Rinda::TupleSpace.new
    @@tss ||= DRb.start_service(Skynet::CONFIG[:TUPLESPACE_DRBURIS].first, @ts)
    mq.clear_outstanding_tasks
    
    @worker_message = Skynet::Message.new(
      :tasktype=> :task, 
      :drburi=> "localhost", 
      :job_id => 1,
      :task_id => 2,
      :payload => "task",
      :payload_type => "master",
      :expiry => 20, 
      :expire_time => 0,
      :iteration => 0,
      :name => "test",       
      :version => 1
    )
    
  end
                   
  def test_tuple_space
    @ts.write([:test])
    assert_equal [:test], @ts.take([:test])
    
    rts = DRbObject.new(nil, Skynet::CONFIG[:TUPLESPACE_DRBURIS].first)
    rts.write([:test])
    assert_equal [:test], rts.take([:test])
  end

  def test_tuplespace_ranges
    tuple = [:test,10]
    @ts.write(tuple)
    assert_equal tuple, @ts.take([:test,0...11],0.00001)
    write_tuple = [:task,"localhost", 2,1,"task","master","test",0.001,1195078587.001,1,1]
    read_tuple = [:task, nil, nil, nil, nil, nil, nil, nil, 0...1195078588, 0..6, 1]
    assert_equal write_tuple.size, read_tuple.size
    @ts.write(write_tuple)
    assert_equal write_tuple, @ts.take(read_tuple,0.00001)    
    
    write_tuple = [:task, "localhost", 2, 1, "task", "master", "test", 0.001, 1195079557.001, 1, 1]
    read_tuple = [:task, nil, nil, nil, nil, nil, nil, nil, 0..1195079558, 0..6, 1]
    assert_equal write_tuple.size, read_tuple.size
    @ts.write(write_tuple)
    assert_equal write_tuple, @ts.take(read_tuple,0.00001)        
  end

### THE FOLLOWING TESTS THE MessageQueueProxy API
  
  def test_write_message    
    assert mq.write_message(@worker_message,10)
    assert_equal 1, mq.list_tasks.size
  end

  def test_take_next_task
    assert mq.write_message(@worker_message,10)
    assert_equal @worker_message.to_a, mq.take_next_task(1).to_a
  end

  
  def test_task_failover
    message = @worker_message.clone
    message.expiry=0.4
    assert mq.write_message(message)
    assert_equal message.to_a, mq.take_next_task(1).to_a
    sleep 0.6
    ntt = Skynet::Message.next_task_template(1)
    assert_equal 2, mq.take_next_task(1,0.00001).task_id
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
      :processed  => 0,
      :version    => 1,
      :started_at => Time.now.to_i
    })        
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
  end
  

  

private
  def mq
		Skynet::MessageQueueAdapter::TupleSpace.new
  end
    
end