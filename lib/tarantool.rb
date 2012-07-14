require 'eventmachine'
require "iproto"
require "tarantool/version"
require "tarantool/request"
require "tarantool/response"
require "tarantool/space_array.rb"
require "tarantool/space_hash.rb"
require "tarantool/query.rb"
require "tarantool/serializers.rb"

module Tarantool
  #autoload :Record, 'tarantool/record'
  #autoload :LightRecord, 'tarantool/light_record'

  class << self
    def new(conf)
      case conf[:type] || :block
      when :em, :em_fiber
        require 'tarantool/fiber_db'
        FiberDB.new(conf[:host], conf[:port])
      when :em_cb, :em_callback
        require 'tarantool/callback_db'
        CallbackDB.new(conf[:host], conf[:port])
      when :block
        require 'tarantool/block_db'
        BlockDB.new(conf[:host], conf[:port])
      end
    end
  end

  class DB
    attr_reader :closed, :connection
    alias closed? closed
    def initialize(host, port)
      @host = host
      @port = port
      @closed = false
      establish_connection
    end

    # returns regular space, where fields are named by position
    #
    # tarantool.space_block(0, [:int, :str, :int, :str], keys: [[0], [1,2]])
    def space_array(space_no, field_types = [], opts = {})
      indexes = opts[:keys] || opts[:indexes]
      self.class::SpaceArray.new(self, space_no, field_types, indexes)
    end
    # alias space_array to space for backward compatibility
    alias space space_array

    def space_hash(space_no, fields, opts = {})
      indexes = opts[:keys] || opts[:indexes]
      self.class::SpaceHash.new(self, space_no, fields, indexes)
    end

    def query
      @query ||= self.class::Query.new(self)
    end

    def method_missing(name, *args)
      if query.respond_to?(name)
        query.send(name, *args)
      else
        super
      end
    end

    def close
      @closed = true
      close_connection
    end

    def establish_connection
      raise NoMethodError, "#establish_connection should be redefined"
    end

    def close_connection
      raise NoMethodError, "#close_connection should be redefined"
    end

    def _send_request(request_type, body, cb)
      raise NoMethodError, "#_send_request should be redefined"
    end

  end
end
