#!/usr/bin/env ruby -w

require 'rinda/ring'
require 'rinda/tuplespace'
require 'rubygems'
require 'logger'
require 'optparse'
require 'pp'

class Rinda::TupleSpaceProxy
  def take(tuple, sec=nil, &block)
    port = []
    port.push @ts.move(nil, tuple, sec, &block)
    port[0]
  end
end

class Rinda::Tuple

  require 'ostruct'

  def init_with_ary(ary)
    if ary.instance_of?(DRb::DRbUnknown)
      begin
        Marshal.load(ary.buf)
      rescue Exception => e
        raise Rinda::RindaError.new("DRb couldn't marshall tuple of type #{ary.name}, it was turned into a DRb::DRbUnknown object.\nMarshal exception #{e.inspect}\nOriginal object:\n\t#{ary.buf}.\n\nStacktrace:\n")
      end
    else
      @tuple = Array.new(ary.size)
      @tuple.size.times do |i|
        @tuple[i] = ary[i]
      end
    end
  end
end

class Skynet
  class Task
  end
  class Message
    class Payload
    end
  end

  class AsyncJob
  end

  class Job
  end

  class TuplespaceServer

    def self.start(options)
      log = Logger.new(options[:logfile])
      log.level = Object.module_eval("#{"Logger::" + options[:loglevel].upcase}", __FILE__, __LINE__)
      log.info "STARTING TUPLESPACE SERVER ON PORT: #{options[:port]} Logging to #{options[:logfile]}"

      # Create a TupleSpace to hold named services, and start running
      begin
        ts = Rinda::TupleSpace.new
        if options[:drburi]
          DRb.start_service(options[:drburi], ts)          
        else
          DRb.start_service
        end
        if options[:use_ringserver] and options[:port] 
          tuple = [:name,:TupleSpace, ts, 'Tuple Space']
          renewer = Rinda::SimpleRenewer.new
          ring_ts = Rinda::TupleSpace.new
          ring_ts.write(tuple, renewer)

          server = Rinda::RingServer.new(ring_ts, options[:port])
        end
        DRb.thread.join
      rescue SystemExit, Interrupt
      rescue Exception, RuntimeError => e
        log.fatal "Couldn't start Skynet Server #{e.inspect}"
      end

    end
  end
end
