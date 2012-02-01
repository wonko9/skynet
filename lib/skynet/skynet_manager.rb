require 'yaml'

class Skynet
  class Manager

    class Error < StandardError
    end

    include SkynetDebugger

    Skynet::CONFIG[:PERCENTAGE_OF_TASK_ONLY_WORKERS]    ||= 0.7
    Skynet::CONFIG[:PERCENTAGE_OF_MASTER_ONLY_WORKERS]  ||= 0.2

    def self.debug_class_desc
      "MANAGER"
    end

    attr_accessor :required_libs, :queue_id
    attr_reader   :config, :worker_queue, :wqts

    def initialize(options)
      raise Error.new("You must provide a script path to Skynet::Manager.new.") unless options[:script_path]
      @script_path          = options[:script_path] || Skynet::CONFIG[:LAUNCHER_PATH]
      # info "Skynet Launcher Path: [#{@script_path}]"
      @workers_requested    = options[:workers]  || 4
      @required_libs        = options[:required_libs]   || []
      @queue_id             = options[:queue_id] || 0
      @number_of_workers    = 0
      @workers_by_type      = {:master => [], :task => [], :any => []}
      @signaled_workers     = []
      @worker_queue         = {}
      @workers_restarting   = 0
      @all_workers_started  = false
      @config               = Skynet::Config.new
      @mutex                = Mutex.new
      @wqts                 = Queue.new
    end

    def worker_notify(item)
      @wqts.push(item)
    end

    def start_worker_queue_thread
      Thread.new do
        last_save_time = Time.now
        loop do
          task = @wqts.pop
          begin
            status = Skynet::WorkerStatusMessage.new(task)
            status.started_at = status.started_at.to_i
            @mutex.synchronize do
              @worker_queue[status.worker_id] = status
            end
            if last_save_time < Time.now - 60
              save_worker_queue_to_file
              last_save_time = Time.now
            end
          rescue Exception => e
            error "Error in worker queue thread #{e.inspect} #{e.backtrace.join("\n")}"
          end
        end
      end
    end

    def start_workers
      load_worker_queue_from_file
      start_worker_queue_thread

      setup_signals

      starting = workers_to_start(@workers_requested)
      warn "Starting #{starting} workers.  QUEUE: #{config.queue_name_by_id(queue_id)} #{@workers_requested - starting} already running."
      add_worker(starting)
    end

    ### maybe workers_to_start should be a method
    def workers_to_start(workers_to_start)
      if not worker_pids.empty?
        worker_pids.each do |worker_pid|
          if worker_alive?(worker_pid)
            @number_of_workers  += 1
            workers_to_start    -= 1
          else
            mark_worker_as_stopped(worker_pid)
          end
          return 0 if workers_to_start < 1
        end
      end
      return workers_to_start
    end

    def check_started_workers
      begin
        100.times do |ii|
          warn "Checking started workers, #{active_workers.size} out of #{@number_of_workers} after the #{(ii+1)}th try..."
          break if active_workers.size >= @number_of_workers
          sleep (@number_of_workers - active_workers.size)
        end
      rescue Exception => e
        fatal "Something bad happened #{e.inspect} #{e.backtrace.join("\n")}"
      end

      @all_workers_started = true

      printlog "FINISHED STARTING ALL #{active_workers.size} WORKERS"
      if active_workers.size > @number_of_workers
        warn "EXPECTED #{@number_of_workers}"
        @number_of_workers = active_workers.size
      end
    end

# the main application loop
    def run
      loop do
        next unless @all_workers_started
        begin
          check_workers
          sleep Skynet::CONFIG[:WORKER_CHECK_DELAY]
        rescue SystemExit, Interrupt => e
          printlog "Manager Exiting!"
          exit
        rescue Exception => e
          fatal "Something bad happened #{e.inspect} #{e.backtrace.join("\n")}"
        end
      end
    end

    def check_workers
      debug "Checking on #{@number_of_workers} workers..." unless @shutdown
      check_running_pids
      check_number_of_workers
      true
    end

    def check_running_pids
      worker_pids.each do |wpid|
        if not worker_alive?(wpid)
          if @shutdown
            info "Worker #{wpid} shut down gracefully.  Removing from queue."
          else
            error "Worker #{wpid} was in queue and but was not running.  Removing from queue."
          end
          mark_worker_as_stopped(wpid)
          @number_of_workers -= 1
        end
      end
      worker_pids
    end

    def check_number_of_workers
      if @shutdown
        worker_shutdown
        if worker_pids.size < 1
          exit
        end
      elsif @workers_restarting > 0
        if @workers_requested - worker_pids.size != 0
          restarting = @workers_requested - worker_pids.size
          warn "RESTART MODE: Expected #{@number_of_workers} workers.  #{worker_pids.size} running. #{restarting} are still restarting"
        else
          warn "RESTART MODE: Expected #{@number_of_workers} workers.  #{worker_pids.size} running."
        end
        @workers_restarting = @workers_requested - worker_pids.size

      elsif worker_pids.size != @number_of_workers
        starting = 0
        if worker_pids.size.to_f / @workers_requested.to_f < 0.85
          starting = @workers_requested - worker_pids.size
          error "Expected #{@number_of_workers} workers.  #{worker_pids.size} running. Starting #{starting}"
          @number_of_workers = worker_pids.size
          add_worker(starting)
        else

          error "Expected #{@number_of_workers} workers.  #{worker_pids.size} running."
          @number_of_workers = worker_pids.size
        end
      end
    end

    def worker_shutdown
      if not @masters_dead
        workers_to_kill = active_workers.select do |w|
          w.map_or_reduce == "master" and active_workers.detect{|status| status.process_id == w.process_id and worker_alive?(w.process_id)}
        end
        warn "Shutting down masters.  #{worker_pids.size} workers still running." if worker_pids.size > 0

        worker_pids_to_kill = workers_to_kill.collect { |w| w.process_id }
        if worker_pids_to_kill and not worker_pids_to_kill.empty?
          warn "FOUND MORE RUNNING MASTERS WE HAVEN'T KILLED:", worker_pids_to_kill
          remove_worker(worker_pids_to_kill)
        end

        if not active_workers.detect { |w| w.map_or_reduce == "master" }
          signal_workers("TERM")
          @masters_dead = true
        else
          return check_number_of_workers
        end
      end
      if worker_pids.size < 1
        info "No more workers running."
      else
        warn "Shutting down.  #{worker_pids.size} workers still running." if worker_pids.size > 0
      end
    end

    def worker_alive?(worker_pid)
      Skynet.process_alive?(worker_pid)
    end

    def add_workers(*args)
      add_worker(*args)
    end

    def add_worker(workers=1)
      num_task_only_workers = (workers * Skynet::CONFIG[:PERCENTAGE_OF_TASK_ONLY_WORKERS]).to_i
      num_master_only_workers = (workers * Skynet::CONFIG[:PERCENTAGE_OF_MASTER_ONLY_WORKERS]).to_i
      warn "Adding #{workers} WORKERS. Task Workers: #{num_task_only_workers}, Master Workers: #{num_master_only_workers} Master & Task Workers: #{workers - num_task_only_workers - num_master_only_workers}"

      @all_workers_started = false
      worker_types = {:task => 0, :master => 0, :any => 0}
      (1..workers).collect do |ii|
        worker_type = :any
        if (ii <= num_master_only_workers)
          worker_type = :master
          worker_types[:master] += 1
        elsif (ii > num_master_only_workers and ii <= num_master_only_workers + num_task_only_workers)
          worker_type = :task
          worker_types[:task] += 1
        else
          worker_types[:any] += 1
        end
        cmd = "#{@script_path} --worker_type=#{worker_type}"
        cmd << " --config='#{Skynet::CONFIG[:CONFIG_FILE]}'" if Skynet::CONFIG[:CONFIG_FILE]
        cmd << " --queue_id=#{queue_id}"
        cmd << " -r #{required_libs.join(' -r ')}" if required_libs and not required_libs.empty?
        wpid = Skynet.fork_and_exec(cmd)
        Skynet.close_console
        @workers_by_type[worker_type] ||= []
        @workers_by_type[worker_type] << wpid
        warn "Adding Worker ##{ii} PID: #{wpid} QUEUE: #{queue_id}, WORKER_TYPE?:#{worker_type}"
        @mutex.synchronize do
          @number_of_workers += 1
        end
        sleep 0.01
        wpid
      end
      info "Worker Distribution", worker_types
      check_started_workers
    end

    def remove_workers(workers=1)
      pids = worker_pids[0...workers]
      remove_worker(pids)
    end

    def remove_worker(pids = nil)
      pids = [pids] unless pids.kind_of?(Array)
      info "Removing workers #{pids.join(",")} from worker queue.  They will die gracefully when they finish what they're doing."
      pids.collect do |wpid|
        Process.kill("INT",wpid)
        mark_worker_as_stopped(wpid)
        @number_of_workers -= 1
        warn "REMOVING WORKER #{wpid}"
        @signaled_workers << wpid
      end
      pids
    end

    def mark_worker_as_stopped(wpid)
      worker = @worker_queue.values.detect {|status| status.process_id == wpid}
      if worker and not worker_alive?(wpid)
        @worker_queue.delete_if {|worker_id, status| status.process_id == wpid }
        worker_pids.delete(worker.process_id)
        worker.started_at = Time.now.to_f
        worker.process_id = nil
      end
    end

    def signal_workers(signal,worker_type=[])
      worker_types = [worker_type].flatten
      active_workers.each do |worker|
        worker_types.each do |worker_type|
          if worker_type == :idle
            next if worker_type and worker.task_id
          else
            next if worker_type and not @workers_by_type[worker_type].include?(worker.process_id)
          end
        end
        warn "SHUTTING DOWN #{worker.process_id} MR: #{worker.map_or_reduce} SIG: #{signal}"
        begin
          Process.kill(signal,worker.process_id)
        rescue Errno::ESRCH
          warn "Tried to kill a process that didn't exist #{worker.process_id}"
        end
        # mark_worker_as_stopped(worker.process_id)
        @signaled_workers << worker.process_id
      end
    end

    def hard_restart_workers
      @all_workers_started = false
      signal_workers("TERM")
      @restart = true
      signal_workers("INT",:master)
      signal_workers("INT",:any)
      sleep @number_of_workers
      check_started_workers
    end

# ===========================
# = XXX THIS IS A HORRIBLE HACK =
# ===========================
    def restart_worker(wpid)
      info "RESTARTING WORKER #{wpid}"
      @mutex.synchronize do
        Process.kill("HUP",wpid)
        mark_worker_as_stopped(wpid)
        @workers_restarting += 1
      end
      sleep Skynet::CONFIG[:WORKER_CHECK_DELAY]
    end

    def restart_workers
      @all_workers_started = false
      signal_workers("HUP")
      sleep @number_of_workers
      check_started_workers
    end

    def setup_signals
      Signal.trap("HUP")  do
        restart_workers
      end
      Signal.trap("TERM") do
        if @term
          terminate
        else
          @term=true
          shutdown
        end
      end

      Signal.trap("INT") do
        if @shutdown
          terminate
        else
          shutdown
        end
      end
    end

    def shutdown
      info(:shutdown)
      @shutdown = true
      signal_workers("TERM",[:idle,:master,:any])
    end

    def terminate
      info(:terminate)
      signal_workers("KILL")
      sleep 1
      exit
    end

    def save_worker_queue_to_file
      debug "Writing worker queue to file #{Skynet.config.manager_statfile_location}"
      File.open(Skynet.config.manager_statfile_location,"w") do |f|
        f.write(YAML.dump(@worker_queue))
      end
    end

    def load_worker_queue_from_file
      if File.exists?(Skynet.config.manager_statfile_location)
        File.open(Skynet.config.manager_statfile_location,"r") do |f|
          begin
            @worker_queue = YAML.load(f.read)
            raise Error.new("Bad Manager File returned type #{@worker_queue.class}") unless @worker_queue.is_a?(Hash)
          rescue Exception => e
            error "Error loading manager stats file: #{f}", e
            @worker_queue = {}
            save_worker_queue_to_file
          end
        end
      end
    end

    def prune_inactive_worker_stats
      @worker_queue.delete_if{|worker_id, worker| !worker.process_id.is_a?(Fixnum) }
      stats
    end

    def self.stats_for_hosts(manager_hosts=nil)
      manager_hosts ||= Skynet::CONFIG[:MANAGER_HOSTS] || ["localhost"]
      stats = {
        :servers           => {},
        :processed         => 0,
        :number_of_workers => 0,
        :active_workers    => 0,
        :idle_workers      => 0,
        :hosts             => 0,
        :masters           => 0,
        :taskworkers       => 0,
        :time              => Time.now.to_f
      }
      servers = {}
      manager_hosts.each do |manager_host|
        begin
          manager = DRbObject.new(nil,"druby://#{manager_host}:#{Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_PORT]}")
          manager_stats = manager.stats
          servers[manager_host] = manager_stats
          manager_stats.each do |key,value|
            next unless value.is_a?(Fixnum)
            stats[key] ||= 0
            stats[key] += value
          end
        rescue DRb::DRbConnError, Errno::ECONNREFUSED  => e
          warn "Couldn't get stats from manager at druby://#{manager_host}:#{Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_PORT]}"
        end
      end
      stats[:servers] = servers
      stats[:hosts]   = manager_hosts
      stats
    end

    def stats
      started_times   = @worker_queue.values.collect{|worker| worker.started_at }.sort
      active_started_times   = active_workers.collect{|worker|worker.started_at }.sort
      stats = {
        :hostname                    => hostname,
        :earliest_update             => started_times.first,
        :latest_update               => started_times.last,
        :active_earliest_update      => active_started_times.first,
        :active_latest_update        => active_started_times.last,
        :processed                   => 0,
        :processed_by_active_workers => 0,
        :number_of_workers           => 0,
        :idle_workers                => 0,
        :shutdown_workers            => 0,
      }
      @worker_queue.values.collect{|worker|stats[:processed] += worker.processed}
      active_workers.collect{|worker|stats[:processed_by_active_workers] += worker.processed}
      currently_active_workers, idle_workers = active_workers.partition{|worker| worker.map_or_reduce }
      stats[:number_of_workers]             = active_workers.size
      stats[:active_workers]                = currently_active_workers.size
      stats[:idle_workers]                  = idle_workers.size
      stats[:shutdown_workers]              = inactive_workers.size
      stats[:masters]                       = active_workers.select{|worker|worker.tasktype.to_s == "master"}.size
      stats[:master_or_task_workers]        = active_workers.select{|worker|worker.tasktype.to_s == "any"}.size
      stats[:taskworkers]                   = active_workers.select{|worker|worker.tasktype.to_s == "task"}.size
      stats[:active_masters]                = currently_active_workers.select{|worker|worker.tasktype.to_s == "master"}.size
      stats[:active_master_or_task_workers] = currently_active_workers.select{|worker|worker.tasktype.to_s == "any"}.size
      stats[:active_taskworkers]            = currently_active_workers.select{|worker|worker.tasktype.to_s == "task"}.size
      stats[:idle_masters]                  = idle_workers.select{|worker|worker.tasktype.to_s == "master"}.size
      stats[:idle_master_or_task_workers]   = idle_workers.select{|worker|worker.tasktype.to_s == "any"}.size
      stats[:idle_taskworkers]              = idle_workers.select{|worker|worker.tasktype.to_s == "task"}.size
      stats
    end

    def active_workers
      @worker_queue.values.select{|status| status.process_id.is_a?(Fixnum) }
    end

    def inactive_workers
      @worker_queue.values.select{|status| !status.process_id.is_a?(Fixnum) }
    end

    def worker_pids
      active_workers.collect {|w| w.process_id}
    end

    def parent_pid
      $$
    end

    def hostname
      @machine_name ||= Socket.gethostname
    end

    def ping
      true
    end

    def self.local_manager_uri
      "druby://localhost:#{Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_PORT]}"
    end

    def self.get
      DRbObject.new(nil,local_manager_uri)
    end

    def self.start(options={})
      options[:add_workers]    ||= nil
      options[:remove_workers] ||= nil
      options[:use_rails]      ||= false
      options[:required_libs]  ||= []

      config = Skynet::Config.new

      OptionParser.new do |opt|
        opt.banner = %{Usage:
        > skynet [options]

        OR to daemonize

        > skynet [options] start
        > skynet stop

        You can also run:
        > skynet console [options]
        }
        opt.on('--restart-all-workers', 'Restart All Workers') do |v|
          puts "Restarting ALL workers on ALL machines."
          begin
            manager = self.get
            manager.restart_all_workers
            exit
          rescue DRb::DRbConnError => e
            puts "No manager running at #{local_manager_uri}  ERROR: #{e.inspect}"
            exit
          end
        end
        opt.on('--restart-workers', 'Restart Workers') do |v|
          puts "Restarting workers on this machine."
          begin
            manager = self.get
            manager.restart_workers
            exit
          rescue DRb::DRbConnError => e
            puts "No manager running at #{local_manager_uri}  ERROR: #{e.inspect}"
            exit
          end
        end
        opt.on('--increment-worker-version', 'Increment Worker Version') do |v|
          ver = Skynet::MessageQueue.new.increment_worker_version
          puts "Incrementing Worker Version to #{ver}"
          exit
        end
        opt.on('--add-workers=WORKERS', 'Number of workers to add.') do |v|
          options[:add_workers] = v.to_i
        end
        opt.on('--remove-workers=WORKERS', 'Number of workers to remove.') do |v|
          options[:remove_workers] = v.to_i
        end
        opt.on('--workers=WORKERS', 'Number of workers to start.') do |v|
          options[:workers] = v.to_i
        end
        opt.on('-r', '--required LIBRARY', 'Require the specified libraries') do |v|
          options[:required_libs] << File.expand_path(v)
        end
        opt.on('--config=CONFIG_FILE', 'Where to find the skynet.rb config file') do |v|
          options[:config_file] = File.expand_path(v)
        end
        opt.on('--queue=QUEUE_NAME', 'Which queue should these workers use (default "default").') do |v|
          options[:queue] = v
        end
        opt.on('--queue_id=queue_id', 'Which queue should these workers use (default 0).') do |v|
          options[:queue_id] = v.to_i
        end
        opt.parse!(ARGV)
      end
      if options[:queue]
        if options[:queue_id]
          raise Skynet::Error.new("You may either provide a queue_id or a queue, but not both.")
        end
        options[:queue_id] = config.queue_id_by_name(options[:queue])
      else
        options[:queue_id] ||= 0
      end

      options[:required_libs].each do |adlib|
        begin
          require adlib
        rescue MissingSourceFile => e
          error "The included lib #{adlib} was not found: #{e.inspect}"
          exit
        end
      end

      options[:config_file] ||= Skynet::CONFIG[:CONFIG_FILE]
      if options[:config_file]
        begin
          require options[:config_file]
        rescue MissingSourceFile => e
          error "The config file at #{options[:config_file]} was not found: #{e.inspect}"
          exit
        end
      elsif Skynet::CONFIG[:SYSTEM_RUNNER]
        error "Config file missing. Please add a config/skynet_config.rb before starting."
      end

      options[:workers]        ||= Skynet::CONFIG[:NUMBER_OF_WORKERS] || 4
      options[:pid_file]       ||= Skynet::Config.pidfile_location
      options[:script_path]    ||= Skynet::CONFIG[:LAUNCHER_PATH]

      # Handle add or remove workers
      if options[:add_workers] or options[:remove_workers]
        begin
          manager = self.get
          if options[:add_workers]
            pids = manager.add_worker(options[:add_workers])
            warn "ADDING #{options[:add_workers]} workers PIDS: #{pids.inspect}"
          elsif options[:remove_workers]
            pids = manager.remove_workers(options[:remove_workers])
            warn "REMOVING #{options[:remove_workers]} workers PIDS: #{pids.inspect}"
          end
        rescue DRb::DRbConnError => e
          warn "Couldnt add or remove workers. There are probably no workers running. At least I couldn't find a skynet_manager around at #{local_manager_uri} #{e.inspect}"
        rescue Exception => e
          warn "Couldnt add or remove workers #{e.inspect} #{e.backtrace.join("\n")}"
        end
        exit

      else

        begin
          debug "Making sure there's an available MessageQueue"
          ts = Skynet::MessageQueue.new
        rescue Skynet::ConnectionError => e
          fatal "Couldn't get MessageQueue! #{e.message}"
          raise Skynet::ConnectionError.new("ERROR!  Couldn't get MessageQueue! #{e.message}")
        end

        debug "CONTINUING TO START : There IS an available MessageQueue", options

        begin
          if oldpid = read_pid_file
            errmsg = nil
            if Skynet.process_alive?(oldpid)
              errmsg = "Another Skynet Manager is running at pid: #{oldpid}"
              warn errmsg
              stderr errmsg
              exit
            else
              errmsg =  "Deleting stale pidfile #{Skynet::Config.pidfile_location}"
              warn errmsg
              stderr errmsg
              File.unlink(Skynet::Config.pidfile_location) if File.exist?(Skynet::Config.pidfile_location)
            end
          end

          printlog "STARTING THE MANAGER!!!!!!!!!!! port: #{Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_PORT]}"
          puts "Starting Skynet..."
          printlog "Skynet Stopped"
          if options["daemonize"]
            Skynet.safefork do
              sess_id = Process.setsid
              write_pid_file
              Skynet.close_console
              run_manager(options)
              exit!
            end
          else
            write_pid_file
            run_manager(options)
          end
        rescue SystemExit, Interrupt
        rescue Exception => e
          fatal("Error in Manager.  Manager Dying. #{e.inspect} #{e.backtrace}")
        end
      end
    end

    def self.run_manager(options)
      @manager = Skynet::Manager.new(options)
      @drb_manager = DRb.start_service("druby://:#{Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_PORT]}", @manager)
      @manager.start_workers
      info "MANAGER STARTED ON PORT: #{Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_PORT]}"
      @manager.run
    end

    # stop the daemon, nicely at first, and then forcefully if necessary
    def self.stop(options = {})
      pid = read_pid_file
      if not pid
        puts "The Skynet Manager is not running. No PID found in #{Skynet::Config.pidfile_location}"
        exit
      end
      $stdout.puts "Stopping Skynet"
      printlog "Stopping Skynet"
      Process.kill("TERM", pid)
      180.times { Process.kill(0, pid); sleep(1) }
      Process.kill("TERM", pid)
      180.times { Process.kill(0, pid); sleep(1) }
      $stdout.puts("using kill -9 #{pid}")
      Process.kill("KILL", pid)
    rescue Errno::ESRCH => e
      printlog "Skynet Stopped"
    ensure
      File.unlink(Skynet::Config.pidfile_location) if File.exist?(Skynet::Config.pidfile_location)
    end

    def self.read_pid_file
      pidfile = Skynet::Config.pidfile_location
      File.read(pidfile).to_i if File.exist?(pidfile)
    end

    def self.write_pid_file
      pidfile = Skynet::Config.pidfile_location
      info "Writing PIDFILE to #{pidfile}"
      open(pidfile, "w") {|f| f << Process.pid << "\n"}
      at_exit { File.unlink(pidfile) if read_pid_file == Process.pid }
    end

  end
end
