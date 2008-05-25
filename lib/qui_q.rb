# QuiQ (pronounced Key Queue) is a global Queue system that holds an arbitrary # of Qs, organized by arbitrary hierarchically keys up to 10 keys deep
# It also supports namespaces and named queues

class QuiQ    
  require 'pp'
  require 'rubygems'
  require 'ostruct'
    
  def self.hash_maker(it,max,final=Queue.new)
    return Hash.new if max <= 1
    return final if it == max
    it += 1
    return Hash.new(hash_maker(it,max,final))
  end                    
    
  # Pre-create our store of queues
  numkeys = 10
  @queues ||= hash_maker(0,2, (1..numkeys).collect { |tkeys| hash_maker(1,tkeys) })

  @callbacks ||= hash_maker(0,3,[])
  
  attr_reader :queue, :namespace

  # Takes a queue name and namespace as optional arguments.  All queues are unique to each queue name and namespace.
  def initialize(queue=:_default,namespace=:_default)
    @queue = queue
    @namespace = namespace
  end

  # Messages are stored in unique queues per queue/namespace and indexed by keys.  Keeps can be arbitrarily deep up to
  # 10 keys deep.  
  # So for example you may have a queue indexed under 'PEOPLE' and a separate queue indexed under 'PEOPLE',5,'MESSAGES'
  def enq(message,*keylist)      
    self.class.enq(namespace,queue,message,true,keylist)
  end

  ## deq blocks until it can give you a result
  def deq(*keylist)
    block = lambda {|val|val}
    subscribe_once(*keylist,&block).value
  end
  
  # To subscribe simply give the location (ordered key list) of the queue you want to subscribe to
  # As well as a block that accepts 1 or 2 args
  # If your block only accepts a single argument, we will only pass you the message
  # If your block accepts 2 arguments, we will pass you the message and an acknowledgement object as the second arg
  # You MUST call acknowledgement_object.ack=true or the message will be re-pushed on the queue
  # Each subscriber will get a unique message. In other words, two subscribers will not get the same message
  # unless a subscriber failed to acknowledge a message, in which case other subscribers will continue
  # to get the message until someone has acknowledged
  def subscribe(*keylist,&block)
    raise "Must pass block" unless block_given?

    Thread.new do
      loop do
        val = self.class.deq(namespace,queue,keylist)
        if val
          if block.arity == 2
            ack = OpenStruct.new(:ack => false)
            block.call(val,ack)
            self.class.enq(namespace,queue,val,false,keylist) unless ack.ack
          else
            block.call(val) 
          end
        end
        Thread.pass
      end
    end
  end

  # Works lke subscribe only it will only call your block once
  def subscribe_once(*keylist,&block)
    raise "Must pass block" unless block_given?
    Thread.new do
      loop do
        val = self.class.deq(namespace,queue,keylist)
        if val
          if block.arity == 2
            ack = OpenStruct.new(:ack => false)
            resp = block.call(val,ack)
            self.class.enq(namespace,queue,val,false,keylist) unless ack.ack
            break val
          else
            break block.call(val) 
          end
          break
        end
        Thread.pass
      end
    end
  end
  
  # durable_subscribe takes the same arguments as subscribe.   The difference is that ALL durable subscribers
  # will receive all messages they're subscribing to.
  def durable_subscribe(*keylist,&block)
    raise "Must pass block" unless block_given?
    callbacks[namespace][queue][keylist.join("|")] << block
  end      
  
  alias_method :publish, :enq
  alias_method :pub, :enq
  alias_method :put, :enq
  alias_method :push, :enq
  alias_method :take, :deq
  alias_method :pop, :deq

  private
  
  def callbacks
    self.class.callbacks
  end

  def self.enq(namespace,queue,message,cb,keylist)
    keylist ||= [:_default]
    hash = @queues[namespace][queue][keylist.size]
    q = keylist.inject(hash) do |hash,key|
      # pp "trying queues[#{namespace}][#{queue}][#{ksize}][#{keys.join('][')}]"
      hash[key]
    end                      
    q.push message
    if cb and callbacks[namespace][queue][keylist.join("|")]
      callbacks[namespace][queue][keylist.join("|")].each {|block| block.call(message)}
    end
  end

  def self.deq(namespace,queue,keylist)
    keylist ||= [:_default]
    hash = @queues[namespace][queue][keylist.size]
    q = keylist.inject(hash) do |hash,key|
      hash[key]
    end
    q.pop
  end                 
  
  def self.callbacks
    @callbacks
  end

  def self.run_example
    q = QuiQ.new()   
    q.subscribe(:some,:guy) do |this|
      puts "sub1 got: [#{this}]"
      sleep 0.1
    end
    # 

    q.durable_subscribe(:some,:guy) do |this|
      puts "durable sub1 got: [#{this}]"
    end

    q.put("1",:some,:guy)
    q.put("2",:some,:guy)
    q.put("3",:some,:guy)

    puts "DEQ ",q.deq(:some,:guy)

    q.put("4",:some,:guy)

    q.subscribe(:some,:guy) do |this,ack|
      puts "sub2 got: [#{this}]"
      ack.ack = false
      sleep 0.1
    end
    q.put("5",:some,:guy)

    q.put("guy",:some,:guy)
    puts "DEQ ",q.deq(:some,:guy)
    sleep 1
  end
end

