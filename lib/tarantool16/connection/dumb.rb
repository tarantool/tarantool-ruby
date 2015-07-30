require 'socket'
require_relative 'common'
module Tarantool16
  module Connection
    class Dumb
      include Common

      def initialize(host, opts = {})
        _init_common(host, opts)
        @reconnect_time = now_f - 1
        @socket = nil
        _connect
      end

      def send_request(code, body, cb)
        _connect
        syswrite format_request(code, next_sync, body)
        written = true
        response = _read_response
      rescue ::Errno::EPIPE, Retry => e
        @socket = nil
        if !written && @retry && @reconnect
          @retry = false
          retry
        end
        cb.call Option.error(nil, Disconnected, e.message)
      rescue StandardError => e
        cb.call Option.error(nil, e, nil)
      else
        cb.call response
      end

      def disconnect
        if @socket
          @socket.close rescue nil
          @socket = nil
          @s = 0
        end
      end

      def close
        @reconnect = false
        if @socket
          @socket.close rescue nil
          @socket = false
          @s = 0
        end
      end

      def connected?
        @socket
      end

      def could_be_connected?
        @socket || (@socket.nil? && (@reconnect || @reconnect_time < now_f))
      end

    private
      def _connect
        return if @socket
        unless could_be_connected?
          raise Disconnected, "connection is closed"
        end
        @socket = TCPSocket.new(*host_port)
        @socket.setsockopt(::Socket::IPPROTO_TCP, ::Socket::TCP_NODELAY, 1)
        @retry = @reconnect
        greeting = @socket.read(IPROTO_GREETING_SIZE)
        unless greeting && greeting.bytesize == IPROTO_GREETING_SIZE
          raise Disconnected, "mailformed greeting #{greeting.inspect}"
        end
        @nbuf = "\x00\x00\x00\x00\x00".force_encoding('BINARY')
        parse_greeting greeting
        authenticate if @user
      rescue ::Errno::ECONNREFUSED, ::Errno::EPIPE, Disconnected => e
        @socket = nil
        if !@reconnect
          @socket = false
          @s = 0
        else
          @reconnect_time = now_f + @reconnect_timeout
        end
        raise CouldNotConnect, e.message
      end

      def syswrite(req)
        if @socket.syswrite(req) != req.bytesize
          raise Retry, "Could not write message"
        end
      end

      def authenticate
        syswrite format_authenticate(@user, @passwd, @salt)
        _read_response.raise_if_error!
      end

      def _read_response
        str = @socket.read(5, @nbuf)
        unless str && str.bytesize == 5
          # check if we sent request or not
          begin
            @socket.send("\x00", 0)
          rescue ::Errno::EPIPE
            # if OS knows that socket is closed, then request were not sent
            raise Retry
          else
            # otherwise request were sent
            raise Disconnected, "disconnected while read length"
          end
        end
        n = parse_size(str)
        raise n unless ::Integer === n
        resp = @socket.read(n)
        raise Disconnected, "disconnected while read response" unless resp && resp.bytesize == n
        r = parse_response(resp)
        if r.ok? && r.sync != @s
          raise UnexpectedResponse, "sync mismatch: #{@s} != #{r.sync}"
        end
        r
      end
    end
  end
end
