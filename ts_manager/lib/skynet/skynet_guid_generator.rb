require 'socket'

begin
  require 'fastthread'
rescue LoadError
  require 'thread'
end

class Skynet
  class UniqueDBNumGenerator

    class Config
      attr_accessor :lockfile, :pidfile, :server_num, :pid_id, :use_incremental_ids
    end

    @@config ||= Config.new

    def self.configure
      yield @@config
    end

    def self.server_num(hostname=nil)
      @@config.server_num ||= Socket.gethostname.sum
    end

    def self.pid_id
      $$
    end

    def self.use_incremental_ids
      @@config.use_incremental_ids
    end
  end

  module GuidGenerator

    @@pid_ctr = 0

    def get_unique_id(nodb=nil)

      if defined?(Skynet::CONFIG) and Skynet::CONFIG[:GUID_GENERATOR]
        Skynet::CONFIG[:GUID_GENERATOR].call
      else
        @@pid_id ||= Skynet::UniqueDBNumGenerator.pid_id

        if not  Skynet::UniqueDBNumGenerator.server_num or not @@pid_id
          raise 'SERVER_NUM or PIDID not defined, please check environment.rb for the proper code.'
        end

        Mutex.new.synchronize do
          timeprt = Time.now.to_f - 1186210800   # figure it out
          timeprt = timeprt  * (2 ** 3)
          @@pid_ctr += 1

          guid_parts = [[timeprt,30],[Skynet::UniqueDBNumGenerator.server_num,8],[@@pid_id,14],[@@pid_ctr,12]]
          
          guid = 0
          guid_parts.each do |part, bitlength|
            guid = guid << bitlength
            guid += part.to_i % (2 ** bitlength)
          end
          guid
        end
      end
    end

  end
end
