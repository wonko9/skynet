require 'socket'
# require File.join(File.dirname(__FILE__), '/geni_guid_generator/version')

begin
  require 'fastthread'
rescue LoadError
  puts 'fastthread not installed, using thread instead'
  require 'thread'
end

# String

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
      @@config.server_num ||= Socket.gethostname.sum / 20000
        # hostname = Socket.gethostname unless hostname
        # matched = hostname.match(/^\w+-([\d].*?)\./)
        # if matched
        #   matched[1].to_i
        # else
        #   1
        # end
      # end
    end


    def self.pid_id
      $$
      # pid = 0
      # Lockfile.new(@@config.lockfile) do
      #   if not File.file? @@config.pidfile
      #     FileUtils.touch(@@config.pidfile)
      #   end
      # 
      #   pid = open(@@config.pidfile, 'r'){|f| f.read.to_i}
      #   if pid == 99
      #     pid = 0
      #   else
      #     pid += 1
      #   end
      #   open(@@config.pidfile, 'w'){|f| f << pid}
      # end
      # pid
    end

    def self.use_incremental_ids
      @@config.use_incremental_ids
    end

  end

  module GuidGenerator

    @@pid_ctr = 0
    @@model_ctrs ||= {}

    def get_unique_id(nodb=nil)
      if defined?(Skynet::CONFIG) and Skynet::CONFIG[:GUID_GENERATOR]
        Skynet::CONFIG[:GUID_GENERATOR].call
      else
        @@pid_id ||= Skynet::UniqueDBNumGenerator.pid_id

        if not  Skynet::UniqueDBNumGenerator.server_num or not @@pid_id
          raise 'SERVER_NUM or PIDID not defined, please check environment.rb for the proper code.'
        end

        mutex = Mutex.new
        mutex.synchronize do
          timeprt = Time.now.to_f - 1151107200
          timeprt = timeprt * 10000
          @@pid_ctr += 1
          @@pid_ctr = 0 if @@pid_ctr > 99
          sprintf("%12d%03d%02d%02d", timeprt, Skynet::UniqueDBNumGenerator.server_num, @@pid_id, @@pid_ctr)
        end
      end
    end
  end
end