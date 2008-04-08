require File.dirname(__FILE__) + '/test_helper.rb'

require 'tempfile'
class TestSkynet < Test::Unit::TestCase

  def test_fork_and_exec
    Tempfile.new('control').open
    file = Tempfile.new('fork_exec')
    Skynet.fork_and_exec("/bin/ls -l /proc/$$/fd >#{file.path}")
    sleep 1
    open("#{file.path}", 'r') do |f| 
      lines = f.readlines
      assert_equal 3, lines.grep(/null/).size, "fork_and_exec should redirect 0,1,2 to dev null"
      assert_equal 0, lines.grep(/control/).size, "fork_and_exec should close parent's file descriptors"
    end
  end

end
