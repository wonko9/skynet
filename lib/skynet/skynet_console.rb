class Skynet
  class Console
    
    def self.start(libs=[])
      require 'optparse'
      require 'rubygems'
      require 'skynet'

      irb = RUBY_PLATFORM =~ /(:?mswin|mingw)/ ? 'irb.bat' : 'irb'

      options = { :irb => irb }
      OptionParser.new do |opt|
        opt.banner = "Usage: console [environment] [options]"
        opt.on("--irb=[#{irb}]", 'Invoke a different irb.') { |v| options[:irb] = v }
        opt.parse!(ARGV)
      end
                   
      libs << "irb/completion"
      libs << "rubygems"
      libs << "skynet"
      libs << "skynet_console_helper"

      exec "#{options[:irb]} -r #{libs.join(" -r ")} --simple-prompt"
    end
  end
end