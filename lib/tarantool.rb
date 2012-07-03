require 'eventmachine'
require "iproto"
require "tarantool/version"
require "tarantool/request"
require "tarantool/response"
require "tarantool/space_array.rb"
require "tarantool/space_hash.rb"
require "tarantool/query.rb"

module Tarantool
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
    # tarantool.space_block(0, :int, :str, :int, :str, indexes: [[0], [1,2]])
    def space_array(space_no, *args)
      options = args.pop  if Hash === args.last
      options ||= {}
      fields = args
      fields.flatten!
      primary_key = options[:pk]
      indexes = options[:indexes]
      self.class::SpaceArray.new(self, space_no, fields, primary_key, indexes)
    end

    def space_hash(space_no, fields, opts = {})
      primary_key = opts[:pk]
      indexes = opts[:indexes]
      self.class::SpaceHash.new(self, space_no, fields, primary_key, indexes)
    end

    def query
      @query ||= self.class::Query.new(self)
    end

    def method_missing(name, *args)
      if name =~ /_(cb|blk|fib)$/ && query.respond_to?(name)
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
