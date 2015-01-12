require 'socket'
require_relative 'common'
module Tarantool16
  module Connection
    class Blocking
      include Common

      def initialize(host, opts = {})
        _init_common(host, opts)
        @nbuf = "\x00".b * 5
        _socket
      end

      def send_request(code, body, cb)
        _send format_request(code, next_sync, body)
        cb.call _read_response
      rescue StandardError => e
        cb.call Response.new(nil, e, nil)
      end

      def close
        if @socket
          @socket.close rescue nil
          @socket = nil
        end
      end

    private
      def _socket
        return @socket if @socket
        @socket = TCPSocket.new(*host_port)
        @socket.setsockopt(::Socket::IPPROTO_TCP, ::Socket::TCP_NODELAY, 1)
        greeting = @socket.read(IPROTO_GREETING_SIZE)
        raise Disconnected, "mailformed greeting #{greeting.inspect}" unless greeting && greeting.bytesize == IPROTO_GREETING_SIZE
        parse_greeting greeting
        authenticate if @user
        @socket
      rescue ::Errno::ECONNREFUSED
        close
        sleep(0.1)
        retry
      end

      def authenticate
        _send format_authenticate(@user, @passwd, @salt)
        _read_response
      end

      def _send(str)
        while _socket.syswrite(str) != str.bytesize
          close
        end
      rescue ::Errno::EPIPE
        close
        retry
      end

      def _read_response
        str = @socket.read(5, @nbuf)
        raise Disconnected unless str && str.bytesize == 5
        n = parse_size(str)
        raise n unless ::Integer === n
        resp = @socket.read(n)
        raise Disconnected unless resp && resp.bytesize == n
        r = parse_reponse(resp)
        raise r.error unless r.ok?
        raise UnexpectedResponse, "sync mismatch: #{@s} != #{r.sync}" unless r.sync == @s
        r
      end
    end
  end
end
