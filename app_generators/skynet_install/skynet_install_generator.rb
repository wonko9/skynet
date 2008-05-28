class SkynetInstallGenerator < RubiGen::Base
  
  DEFAULT_SHEBANG = File.join(Config::CONFIG['bindir'],
                              Config::CONFIG['ruby_install_name'])
  
  default_options :in_rails => false
  default_options :mysql    => false
  
  attr_reader :name
  attr_reader :in_rails
  attr_reader :mysql
  
  def initialize(runtime_args, runtime_options = {})
    super
    usage if args.empty?
    @destination_root = File.expand_path(args.shift)
    @name = base_name
    extract_options
  end

  def manifest
    record do |m|

      # Ensure appropriate folder(s) exists
      BASEDIRS.each { |path| m.directory path }
      
      # Create stubs
      m.template  "skynet_config.rb", "config/skynet_config.rb", :collision => :ask, :chmod => 0655
      if @in_rails
        m.directory 'config/initializers'
        m.template  "skynet_initializer.rb", "config/initializers/skynet.rb", :collision => :ask, :chmod => 0655
      end
      if @mysql
        m.directory 'db/migrate'
        m.template   "skynet_mysql_schema.sql", "db/skynet_mysql_schema.sql", :collision => :ask, :chmod => 0655
        m.migration_template "migration.rb", "db/migrate", 
          :collision => :ask, 
          :assigns => {
              :migration_name => "CreateSkynetTables"
          },  :migration_file_name => "create_skynet_tables"
      end
    end
  end

  protected
    def banner
      <<-EOS
Creates a ...

USAGE: #{spec.name} [--rails] [--mysql] directory (can be '.' for current)"
Installs: 
  ./config/skynet_config.rb
EOS
    end

    def add_options!(opts)
      opts.separator ''
      opts.separator 'Options:'
      # For each option below, place the default
      # at the top of the file next to "default_options"
      opts.on("-v", "--version", "Show the #{File.basename($0)} version number and quit.")
      opts.on("--mysql", 
             "Include mysql migration if you want to use mysql as your message queue.  
             Installs:
             ./db/skynet_mysql_schema.sql
             ./db/migrate/db/migrate/###_create_skynet_tables.rb
             ") do |mysql|
               options[:mysql] = true if mysql
             end
      opts.on("-r", "--rails",
              "Install into rails app.
              Installs:
              ./config/initializers/skynet.rb
              (If using rails 1, make sure to add require 'skynet' to your environment.rb)",
              "Default: false") do |rails| 
                options[:rails] = true if rails
              end
    end
    
    def extract_options
      # for each option, extract it into a local variable (and create an "attr_reader :author" at the top)
      # Templates can access these value via the attr_reader-generated methods, but not the
      # raw instance variable value.
      @in_rails = options[:rails]
      @mysql    = options[:mysql]
    end

    # Installation skeleton.  Intermediate directories are automatically
    # created so don't sweat their absence here.
    BASEDIRS = %w(
      config
      log
    )
end