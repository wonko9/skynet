class Skynet
  begin
    require 'fastthread'
  rescue LoadError
    # puts 'fastthread not installed, using thread instead'
    require 'thread'
  end

  class Manager
    include SkynetDebugger

    Skynet::CONFIG[:PERCENTAGE_OF_TASK_ONLY_WORKERS]    ||= 0.7
    Skynet::CONFIG[:PERCENTAGE_OF_MASTER_ONLY_WORKERS]  ||= 0.2
                
    def self.debug_class_desc
      "MANAGER"
    end                  
        

    attr_accessor :required_libs
    
    def initialize(script_path,workers_requested,adlibs=[])
      info "Skynet Launcher Path: [#{@script_path}]"
      @script_path          = script_path
      @mutex                = Mutex.new
      @workers_requested    = workers_requested
      @required_libs        = adlibs
      @number_of_workers    = 0
      @workers_by_type      = {:master => [], :task => [], :any => []}
      @signaled_workers     = []
      @workers_running      = {}
      @all_workers_started  = false                      
    end   
    
    def start_workers
      setup_signals
      
      starting = workers_to_start(@workers_requested)
      warn "Starting #{starting} workers.  #{@workers_requested - starting} already running."
      add_worker(starting)
    end
    
    ### maybe workers_to_start should be a method
    def workers_to_start(workers_to_start)
      pids = worker_queue_pids
      if not pids.empty?      
        pids.each do |worker_pid|
          if worker_alive?(worker_pid)          
            @workers_running[worker_pid] = Time.now
            @number_of_workers  += 1
            workers_to_start    -= 1
          else
            take_worker_status(worker_pid)
          end
          return 0 if workers_to_start < 1
        end
      end
      return workers_to_start
    end

    def check_started_workers
      workers = []                              
      begin
        100.times do |ii|
          workers = worker_queue
          warn "Checking started workers, #{workers.size} out of #{@number_of_workers} after the #{(ii+1)}th try..."
          break if workers.size >= @number_of_workers        
          sleep (@number_of_workers - workers.size)
        end                                          
      rescue Exception => e
        fatal "Something bad happened #{e.inspect} #{e.backtrace.join("\n")}"
      end

      @all_workers_started = true

      warn "FINISHED STARTING ALL #{workers.size} WORKERS"
      if workers.size > @number_of_workers
        warn "EXPECTED #{@number_of_workers}" 
        @number_of_workers = workers.size
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
          fatal "Manager Exiting!"
          exit
        rescue Exception => e
          fatal "Something bad happened #{e.inspect} #{e.backtrace.join("\n")}"
        end
      end
    end
    
    def check_workers
      q_pids = worker_queue_pids || []
      info "Checking on #{@number_of_workers} workers..." unless @shutdown
      check_running_pids(q_pids)
      check_number_of_workers(q_pids)
      true          
    end

    def check_running_pids(q_pids)
      if @workers_running.keys.size > q_pids.size
         (@workers_running.keys - q_pids).each do |wpid|
           error "Missing worker #{wpid} from worker queue. Removing and/or killing."
           Process.kill("TERM",wpid) if worker_alive?(wpid)
           @workers_running.delete(wpid)
           q_pids.delete(wpid)
         end
      end
      
      q_pids.each do |wpid|
        if not worker_alive?(wpid)
          error "Worker #{wpid} was in queue and but was not running.  Removing from queue."
          take_worker_status(wpid)
          @workers_running.delete(wpid)
          @number_of_workers -= 1
          q_pids.delete(wpid)
        end
      end
      q_pids
    end                          
                        
    def worker_shutdown(q_pids)
      if not @masters_dead
        warn "Shutting down masters.  #{q_pids.size} workers still running." if q_pids.size > 0
        workers_to_kill = worker_queue.select do |w| 
          w.map_or_reduce == "master" and @workers_running.include?(w.process_id)
        end                           

        worker_pids_to_kill = workers_to_kill.collect { |w| w.process_id }
        if worker_pids_to_kill and not worker_pids_to_kill.empty?
          warn "FOUND MORE RUNNING MASTERS WE HAVEN'T KILLED:", worker_pids_to_kill                                                    
          remove_worker(worker_pids_to_kill)                                        
        end

        if not worker_queue.detect { |w| w.map_or_reduce == "master" }
          signal_workers("INT")
          @masters_dead = true
          sleep 1
          return check_number_of_workers(worker_queue_pids)
        else
          sleep 4
          return check_number_of_workers(worker_queue_pids)
        end
      else
        warn "Shutting down.  #{q_pids.size} workers still running." if q_pids.size > 0
      end
      if q_pids.size < 1
        info "No more workers running."
      end        
    end      
    
    def check_number_of_workers(q_pids)
      if @shutdown         
        worker_shutdown(q_pids)
        if q_pids.size < 1
          exit
        end        
      elsif q_pids.size != @number_of_workers
        if q_pids.size.to_f / @workers_requested.to_f < 0.85
          starting = @workers_requested - q_pids.size  
          error "Expected #{@number_of_workers} workers.  #{q_pids.size} running. Starting #{starting}"          
          @number_of_workers += starting 
          add_worker(starting)          
        else          
          error "Expected #{@number_of_workers} workers.  #{q_pids.size} running."
          @number_of_workers = q_pids.size
        end
      end

    end
    
    def take_worker_status(worker_process_id)
      begin
        mq.take_worker_status({
          :hostname   => hostname,
          :process_id => worker_process_id
        },0.00001)
      rescue Skynet::QueueTimeout => e
        error "Couldnt take worker status for #{hostname} #{worker_process_id}"
      end
    end 
         
    def worker_alive?(worker_pid)
      begin                   
        IO.popen("ps -o pid,command -p #{worker_pid}", "r") do |ps|
          return ps.detect {|line| line =~ /worker_type/}
        end
      rescue Errno::ENOENT => e
        return false
      end
      false
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
        cmd << " -r #{required_libs.join(' -r ')}" if required_libs and not required_libs.empty?
        wpid = self.fork_and_exec(cmd)
        @workers_by_type[worker_type] ||= []
        @workers_by_type[worker_type] << wpid
        warn "Adding Worker ##{ii} PID: #{wpid} WORKER_TYPE?:#{worker_type}"
        @mutex.synchronize do
          @number_of_workers += 1
        end
        @workers_running[wpid] = Time.now
        sleep 0.01
        wpid
      end                  
      info "DISTRO", worker_types
      check_started_workers 
    end
                    
    def remove_workers(workers=1)
      pids = worker_queue_pids[0...workers]
      remove_worker(pids)
    end

    def remove_worker(pids = nil)
      pids = [pids] unless pids.kind_of?(Array)
      info "Removing workers #{pids.join(",")} from worker queue.  They will die gracefully when they finish what they're doing."
      wq = worker_queue
      pids.collect do |wpid|
        @workers_running.delete(wpid)
        @number_of_workers -= 1
        @workers_running.delete(wpid)      
        warn "REMOVING WORKER #{wpid}"
        # error "SHUTTING DOWN #{wpid} MR:",worker_queue.detect{|w|w.process_id == wpid}
        @signaled_workers << wpid
        Process.kill("INT",wpid)      
      end                       
      pids
    end

    def signal_workers(signal,worker_type=nil)
      worker_queue.each do |worker|
        next if worker_type and not @workers_by_type[worker_type].include?(worker.process_id)
        warn "SHUTTING DOWN #{worker.process_id} MR: #{worker.map_or_reduce}"
        @workers_running.delete(worker.process_id)
        Process.kill(signal,worker.process_id)
        @signaled_workers << worker.process_id
      end
    end 
    
    def restart_all_workers
      hostnames = {}
      mq.read_all_worker_statuses.each do |status|
        hostnames[status.hostname] = true
      end
      hostnames.keys.each do |remote_hostname|
        manager = DRbObject.new(nil,"druby://#{remote_hostname}:40000")
        manager.restart_workers
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

    def restart_workers
      @all_workers_started = false
      signal_workers("HUP")
      @workers_running = {}
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
      signal_workers("INT",:master)
      signal_workers("INT",:any)
    end

    def terminate             
      info(:terminate)                  
      signal_workers("TERM")
      exit
    end

    def fork_and_exec(command)
      pid = fork do                                                                  
        exec("/bin/sh -c \"#{command}\"")
        exit
      end
      Process.detach(pid) if (pid != 0)
      pid
    end

    def mq
      @mq ||= Skynet::MessageQueue.new
    end    

    def worker_queue
      mq.read_all_worker_statuses(hostname)
    end
    
    def worker_queue_pids
      worker_queue.collect {|w| w.process_id}
    end        

    def worker_pids
      worker_queue_pids
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

    def self.start(options={})
      options[:add_workers]    ||= nil
      options[:remove_workers] ||= nil
      options[:use_rails]      ||= false
      options[:required_libs]  ||= []
      OptionParser.new do |opt|
        opt.banner = "Usage: skynet [options]"
        opt.on('', '--restart-all-workers', 'Restart All Workers') do |v| 
          puts "Restarting ALL workers on ALL machines."
          begin
            manager = DRbObject.new(nil, Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_URL])
            manager.restart_all_workers
            exit
          rescue DRb::DRbConnError => e
            puts "No manager running at #{Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_URL]}  ERROR: #{e.inspect}"
            exit
          end
        end
        opt.on('', '--restart-workers', 'Restart Workers') do |v| 
          puts "Restarting workers on this machine."
          begin
            manager = DRbObject.new(nil, Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_URL])
            manager.restart_workers
            exit
          rescue DRb::DRbConnError => e
            puts "No manager running at #{Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_URL]}  ERROR: #{e.inspect}"
            exit
          end
        end
        opt.on('-i', '--increment-worker-version', 'Increment Worker Version') do |v| 
          ver = Skynet::MessageQueue.new.increment_worker_version
          puts "Incrementing Worker Version to #{ver}"
          exit
        end
        opt.on('-a', '--add-workers WORKERS', 'Number of workers to add.') do |v| 
          options[:add_workers] = v.to_i
        end
        opt.on('-k', '--remove-workers WORKERS', 'Number of workers to remove.') do |v| 
          options[:remove_workers] = v.to_i
        end
        opt.on('-w', '--workers WORKERS', 'Number of workers to start.') do |v| 
          options[:workers] = v.to_i
        end               
        opt.on('-r', '--required LIBRARY', 'Require the specified libraries') do |v|
          options[:required_libs] << File.expand_path(v)
        end

        opt.parse!(ARGV)
      end
      
      options[:workers]   ||=  Skynet::CONFIG[:NUMBER_OF_WORKERS] || 4
      options[:pid_file]  ||=  File.dirname(Skynet::CONFIG[:SKYNET_PIDS_FILE]) + "/skynet_worker.pid"

      options[:required_libs].each do |adlib|
        begin
          require adlib
        rescue MissingSourceFile => e
          error "The included lib #{adlib} was not found: #{e.inspect}"
          exit
        end
      end        

      # Handle add or remove workers
      if options[:add_workers] or options[:remove_workers]
        begin
          manager = DRbObject.new(nil, Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_URL])    
          if options[:add_workers]
            pids = manager.add_worker(options[:add_workers])
            warn "ADDING #{options[:add_workers]} workers PIDS: #{pids.inspect}"
          elsif options[:remove_workers]
            pids = manager.remove_workers(options[:remove_workers])
            warn "REMOVING #{options[:remove_workers]} workers PIDS: #{pids.inspect}"
          end
        rescue DRb::DRbConnError => e
          warn "Couldnt add or remove workers. There are probably no workers running. At least I couldn't find a skynet_manager around at #{Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_URL]} #{e.inspect}"
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

        # create main pid file
        File.open(options[:pid_file], 'w') do |file|
          file.puts($$)
        end

        begin                                                                                                                   
          info "STARTING THE MANAGER!!!!!!!!!!!"
          @manager = Skynet::Manager.new(Skynet::CONFIG[:LAUNCHER_PATH],options[:workers],options[:required_libs])
          DRb.start_service(Skynet::CONFIG[:SKYNET_LOCAL_MANAGER_URL], @manager)
          info "WORKER MANAGER URI: #{DRb.uri}"
          @manager.start_workers
          @manager.run
          DRb.thread.join
        rescue SystemExit, Interrupt
        rescue Exception => e
          fatal("Error in Manager.  Manager Dying. #{e.inspect} #{e.backtrace}")        
        end
      end
    end

  end
end
