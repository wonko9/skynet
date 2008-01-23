class Skynet
  class Console    
    def self.start(libs=[])
      require 'rubygems'
      require 'optparse'
      require 'skynet'
      require "skynet_console_helper"
      require "irb/completion"
      require "irb"

      irb = RUBY_PLATFORM =~ /(:?mswin|mingw)/ ? 'irb.bat' : 'irb'

      options = { 
        :irb           => irb, 
        :required_libs => [] 
      }
      
      OptionParser.new do |opt|
        opt.banner = "Usage: skynet console [options]"
        opt.on("--irb=[#{irb}]", 'Invoke a different irb.') { |v| options[:irb] = v }
        opt.on('-r', '--required LIBRARY', 'Require the specified libraries. To include multiple libraries, include multiple -r options. ie. -r skynet -r fileutils') do |v|
          options[:required_libs] << File.expand_path(v)
        end
        opt.parse!(ARGV)
      end
                         
      options[:required_libs] + libs
      options[:required_libs].uniq.each do |lib|
        require lib
      end
      
      IRB.setup(Skynet::CONFIG[:LAUNCHER_PATH])
      IRB.conf[:PROMPT_MODE]  = :SIMPLE
      irb = IRB::Irb.new()
      IRB.conf[:MAIN_CONTEXT] = irb.context      
      irb.context.workspace.main.extend Skynet::ConsoleHelper

      trap("SIGINT") do
        irb.signal_handle
      end

      catch(:IRB_EXIT) do
        irb.eval_input
      end      
    end
  end
end