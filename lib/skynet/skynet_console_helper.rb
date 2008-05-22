module Skynet::ConsoleHelper
# All of these commands can be run at the 'skynet console'.

    def log
      Skynet::Logger.get
    end

    def mq
      @mq ||= Skynet::MessageQueue.new
    end                       

    def stats
      mq.stats
    end

    def increment_worker_version
      mq.increment_worker_version
    end

    def get_worker_version
      mq.get_worker_version
    end

    def set_worker_version(*args)
      mq.set_worker_version(*args)
    end

    def manager
      @manager ||= Skynet::Manager.get
    end

    def add_lib(lib)
      manager.required_libs << File.expand_path(lib)
      manager.restart_workers
    end                      

    def restart_workers
      manager.restart_workers
    end

    def add_workers(num)
      manager.add_workers(num)
    end

    def remove_workers(num)
      manager.remove_workers(num)
    end

    # ===============
    # = Doesnt work =
    # ===============
    # def help
    #   puts <<-HELP
    #    mq
    #    stats
    #    increment_worker_version
    #    get_worker_version
    #    set_worker_version(version)
    #    manager
    #    add_lib(library_to_include)  -- forces a restart
    #    restart_workers
    #    add_workers(number_of_workers)
    #    remove_workers(number_of_workers)
    #   HELP
    # end
end
