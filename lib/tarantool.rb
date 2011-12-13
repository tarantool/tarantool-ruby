# -*- coding: utf-8 -*-
require 'eventmachine'
require 'em-synchrony'

module Tarantool
  VERSION = '0.1'
  extend self
  require 'tarantool/space'
  require 'tarantool/connection'
  require 'tarantool/requests'
  require 'tarantool/response'
  require 'tarantool/exceptions'
  require 'tarantool/serializers'

  def singleton_space
    @singleton_space ||= space
  end

  def connection
    @connection ||= begin
      raise "Tarantool.configure before connect" unless @config
      EM.connect @config[:host], @config[:port], Tarantool::Connection
    end
  end

  def reset_connection
    @connection = nil
    @singleton_space = nil
  end

  def space(no = nil)
    Space.new connection, no || @config[:space_no]
  end

  def configure(config = {})
    EM.add_shutdown_hook { reset_connection }
    @config = config
  end

  def hexdump(string)
    string.unpack('C*').map{ |c| "%02x" % c }.join(' ')
  end

  [:select, :update, :insert, :delete, :call, :ping].each do |v|
    define_method v do |*params|
      singleton_space.send v, *params
    end
  end

end