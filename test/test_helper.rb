ENV["RAILS_ENV"] = "test"
$VERBOSE=false


require 'test/unit'
require 'rubygems'
require File.dirname(__FILE__) + '/../lib/skynet'
require 'pp'
require 'mocha'
require 'functor'
