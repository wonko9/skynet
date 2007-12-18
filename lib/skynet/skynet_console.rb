class Skynet
  class Console    
    def self.start(libs=[])
      require 'rubygems'
      require 'optparse'
      require 'skynet'

      irb = RUBY_PLATFORM =~ /(:?mswin|mingw)/ ? 'irb.bat' : 'irb'

      options = { 
        :irb           => irb, 
        :required_libs => [] 
      }
      
      OptionParser.new do |opt|
        opt.banner = "Usage: skynet_console [options]"
        opt.on("--irb=[#{irb}]", 'Invoke a different irb.') { |v| options[:irb] = v }
        opt.on('-r', '--required LIBRARY', 'Require the specified libraries. To include multiple libraries, include multiple -r options. ie. -r skynet -r fileutils') do |v|
          options[:required_libs] << File.expand_path(v)
        end
        opt.parse!(ARGV)
      end
                   
      libs << "irb/completion"
      libs << "rubygems"
      libs << "skynet"
      libs << "skynet_console_helper"
      libs += options[:required_libs]
      cmd = "#{options[:irb]} -r #{libs.join(" -r ")} --simple-prompt"
            
      exec cmd
    end
  end
end