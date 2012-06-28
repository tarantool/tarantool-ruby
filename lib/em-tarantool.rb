require 'eventmachine'
require "iproto"
require "em-tarantool/version"
require "em-tarantool/space"

module EM
  class Tarantool
    class Error < StandardError; end
    class BadReturnCode < Error; end
    class ValueError < Error; end

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
    # tarantool.space(0, :int, :str, :int, :str, indexes: [[0], [1,2]])
    def space(space_no, *args)
      options = args.pop  if Hash === args.last
      options ||= {}
      fields = args
      indexes = options[:indexes]
      Space.new(self, space_no, fields, indexes)
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
      EM.schedule do
        @connection.send_request(request_type, body, cb)
      end
    end
  end
end