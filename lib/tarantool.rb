require 'eventmachine'
require "iproto"
require "tarantool/version"
require "tarantool/request"
require "tarantool/response"

class Tarantool
  autoload :SpaceArrayBlock, "tarantool/space_array_block"
  autoload :SpaceArrayFiber, "tarantool/space_array_fiber"
  autoload :SpaceHashBlock, "tarantool/space_hash_block"
  autoload :SpaceHashFiber, "tarantool/space_hash_fiber"
  autoload :Query,          "tarantool/query"

  attr_reader :closed, :connection
  alias closed? closed
  def initialize(host, port)
    @host = host
    @port = port
    @closed = false
    @connection_waiters = []
    EM.schedule do
      unless @closed
        @connection = IProto.get_connection(host, port, :em_callback)
        while waiter = @connection_waiters.shift
          _send_request(*waiter)
        end
      end
    end
  end

  # returns regular space, where fields are named by position
  #
  # tarantool.space_block(0, :int, :str, :int, :str, indexes: [[0], [1,2]])
  def space_block(space_no, *args)
    options = args.pop  if Hash === args.last
    options ||= {}
    fields = args
    fields.flatten!
    primary_key = options[:pk]
    indexes = options[:indexes]
    SpaceArrayBlock.new(self, space_no, fields, primary_key, indexes)
  end

  # returns fibered space, where fields are named by position
  #
  # tarantool.space_fiber(0, :int, :str, :int, :str, indexes: [[0], [1,2]])
  def space_fiber(space_no, *args)
    options = args.pop  if Hash === args.last
    options ||= {}
    fields = args
    fields.flatten!
    primary_key = options[:pk]
    indexes = options[:indexes]
    SpaceArrayFiber.new(self, space_no, fields, primary_key, indexes)
  end

  def space_hash_block(space_no, fields, opts = {})
    primary_key = opts[:pk]
    indexes = opts[:indexes]
    SpaceHashBlock.new(self, space_no, fields, primary_key, indexes)
  end

  def space_hash_fiber(space_no, fields, opts = {})
    primary_key = opts[:pk]
    indexes = opts[:indexes]
    SpaceHashFiber.new(self, space_no, fields, primary_key, indexes)
  end

  def query
    @query ||= Query.new(self)
  end

  def method_missing(name, *args)
    if name =~ /_(cb|blk|fib)$/ && query.respond_to?(name)
      query.send(name, *args)
    else
      super
    end
  end

  def close
    EM.schedule do
      @closed = true
      if @connection
        @connection.close
        @connection = nil
      end
      unless @connection_waiters.empty?
        while waiter = @connection_waiters.shift
          waiter.last.call(::IProto::Disconnected)
        end
      end
    end
  end

  def _send_request(request_type, body, cb)
    if @connection
      @connection.send_request(request_type, body, cb)
    else
      @connection_waiters << [request_type, body, cb]
    end
  end
end
