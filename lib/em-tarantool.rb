require 'eventmachine'
require "iproto"
require "em-tarantool/version"
require "em-tarantool/request"
require "em-tarantool/response"
require "em-tarantool/space_base"

module EM
  class Tarantool
    include Request
    include Response
    autoload :SpaceBaseBlock, "em-tarantool/space_base_block"
    autoload :SpaceBaseFiber, "em-tarantool/space_base_fiber"
    autoload :SpaceHashBlock, "em-tarantool/space_hash_block"
    autoload :SpaceHashFiber, "em-tarantool/space_hash_fiber"

    attr_reader :closed, :connection
    alias closed? closed
    def initialize(host, port)
      @host = host
      @port = port
      @closed = false
      EM.schedule do
        unless @closed
          @connection = IProto.get_connection(host, port, :em_callback)
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
      SpaceBaseBlock.new(self, space_no, fields, primary_key, indexes)
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
      SpaceBaseFiber.new(self, space_no, fields, primary_key, indexes)
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

    def close
      EM.schedule do
        @closed = true
        if @connection
          @connection.close
          @connection = nil
        end
      end
    end

    def _send_request(request_type, body, cb)
      @connection.send_request(request_type, body, cb)
    end
  end
end
