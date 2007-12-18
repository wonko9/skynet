class SkynetInstallGenerator < RubiGen::Base
  
  DEFAULT_SHEBANG = File.join(Config::CONFIG['bindir'],
                              Config::CONFIG['ruby_install_name'])
  
  default_options :author             => nil
  default_options :in_rails           => nil
  default_options :include_migration  => false
  
  attr_reader :name
  attr_reader :in_rails
  attr_reader :include_migration
  
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
      m.directory 'script'
      BASEDIRS.each { |path| m.directory path }
      
      # Create stubs
      # m.template "template.rb",  "some_file_after_erb.rb"
      m.template     "skynet",         "script/skynet", :assigns => {:rails => @rails}, :collision => :ask, :chmod => 0755, :shebang => options[:shebang]
      m.template     "skynet_console", "script/skynet_console", :assigns => {:rails => @rails}, :collision => :ask, :chmod => 0755, :shebang => options[:shebang]
      if include_migration
        m.directory 'db/migrate'
        m.migration_template "migration.rb", "db/migrate", :collision => :skip, :assigns => {
              :migration_name => "CreateSkynetTables"
          },  :migration_file_name => "create_skynet_tables"
      end
      # m.dependency "install_rubigen_scripts", [destination_root, 'skynet_install'], 
      #   :shebang => options[:shebang], :collision => :force
    end
  end

  protected
    def banner
      <<-EOS
Creates a ...

USAGE: #{spec.name} [--rails] directory (can be '.' for current)"
EOS
    end

    def add_options!(opts)
      opts.separator ''
      opts.separator 'Options:'
      # For each option below, place the default
      # at the top of the file next to "default_options"
      # opts.on("-a", "--author=\"Your Name\"", String,
      #         "Some comment about this option",
      #         "Default: none") { |options[:author]| }
      opts.on("-v", "--version", "Show the #{File.basename($0)} version number and quit.")
      opts.on("--include-migration", 
             "Include mysql migration if you want to use mysql as your message queue") { |options[:include_migration]| }
      opts.on("-r", "--rails",
              "Install into rails app",
              "Default: false") { |options[:rails]| }
    end
    
    def extract_options
      # for each option, extract it into a local variable (and create an "attr_reader :author" at the top)
      # Templates can access these value via the attr_reader-generated methods, but not the
      # raw instance variable value.
      # @author = options[:author]
      @in_rails       = options[:rails]
      @include_migration = options[:include_migration]
    end

    # Installation skeleton.  Intermediate directories are automatically
    # created so don't sweat their absence here.
    BASEDIRS = %w(
      log
      script
      db/migrate
    )
end