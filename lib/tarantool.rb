# -*- coding: utf-8 -*-
require 'iproto'

class Tarantool
  VERSION = '0.2.5'
  
  require 'tarantool/space'
  require 'tarantool/requests'
  require 'tarantool/response'
  require 'tarantool/exceptions'
  require 'tarantool/serializers'

  attr_reader :config, :closed
  alias closed? closed
  def initialize(config = {})
    @config = config
    @closed = false
  end

  def configure(config)
    @config.merge! config
  end

  def connection(c = config)
    return @connection  if @closed
    @connection ||= begin
      raise "Tarantool.configure before connect" unless c
      IProto.get_connection c[:host], c[:port], c[:type] || :block
    end
  end

  def space(no, conn = connection)
    Space.new conn, no
  end

  def close
    @closed = true
    if @connection
      @connection.close
      @connection = nil
    end
  end

  def self.hexdump(string)
    string.unpack('C*').map{ |c| "%02x" % c }.join(' ')
  end
end
