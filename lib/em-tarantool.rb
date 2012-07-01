require 'eventmachine'
require "iproto"
require "em-tarantool/version"
require "em-tarantool/request"
require "em-tarantool/response"
require "em-tarantool/space_block"

module EM
  class Tarantool
    include Request
    include Response
    autoload :SpaceFiber, "em-tarantool/space_fiber"

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
      SpaceBlock.new(self, space_no, fields, primary_key, indexes)
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
