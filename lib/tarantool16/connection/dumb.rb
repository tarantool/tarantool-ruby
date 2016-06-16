require 'socket'
require 'io/wait'
begin
  #require 'kgio'
rescue LoadError
end

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
        @socket.close rescue nil
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
        if  @host.rindex(/unix:\/.+/) == 0
          @socket = Socket.unix(@host.match('unix:/*(/.+)')[1])
        else
          @socket = Socket.new((_ipv6? ? Socket::AF_INET6 : Socket::AF_INET), Socket::SOCK_STREAM)
          @socket.setsockopt(::Socket::IPPROTO_TCP, ::Socket::TCP_NODELAY, 1)
          @socket.sync = true
          sockaddr = Socket.pack_sockaddr_in(*host_port.reverse)
          @retry = @reconnect
          if @timeout
            _connect_nonblock(sockaddr)
          else
            @socket.connect(sockaddr)
          end
        end

        greeting = _read(IPROTO_GREETING_SIZE)
        unless greeting && greeting.bytesize == IPROTO_GREETING_SIZE
          raise Disconnected, "mailformed greeting #{greeting.inspect}"
        end
        @nbuf = "\x00\x00\x00\x00\x00".force_encoding('BINARY')
        parse_greeting greeting
        authenticate if @user
      rescue ::Errno::ECONNREFUSED, ::Errno::EPIPE, Disconnected, Timeout => e
        @socket.close rescue nil
        @socket = nil
        if !@reconnect
          @socket = false
          @s = 0
        else
          @reconnect_time = now_f + @reconnect_timeout
        end
        raise CouldNotConnect, e.message
      end

      def _connect_nonblock(sockaddr)
        expire = now_f + @timeout
        begin
          @socket.connect_nonblock(sockaddr)
        rescue IO::WaitWritable
          t = [@socket]
          IO.select(t, t, nil, expire - now_f)
          begin
            @socket.connect_nonblock(sockaddr)
          rescue Errno::EISCONN
          end
        end
      end

      def syswrite(req)
        unless @timeout
          if @socket.syswrite(req) != req.bytesize
            raise Retry, "Could not write message"
          end
        else
          expire = now_f
          begin
            until req.empty?
              n = @socket.write_nonblock(req)
              req = req[n..-1]
            end
          rescue IO::WaitWritable
            _wait_writable(expire - now_f)
          rescue Errno::EINTR
            retry
          end
        end
      end

      def authenticate
        syswrite format_authenticate(@user, @passwd, @salt)
        _read_response.raise_if_error!
      end

      def _read_response
        str = _read(5, @nbuf)
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
        resp = _read(n)
        raise Disconnected, "disconnected while read response" unless resp && resp.bytesize == n
        r = parse_response(resp)
        if r.ok? && r.sync != @s
          raise UnexpectedResponse, "sync mismatch: #{@s} != #{r.sync}"
        end
        r
      end

      def _read(n, buf=nil)
        unless @timeout
          buf ? @socket.read(n, buf) : @socket.read(n)
        else
          expire = now_f + @timeout
          rbuf = nil
          while n > 0
            case tbuf = _read_nonblock(n, buf)
            when String
              if rbuf
                rbuf << tbuf
              else
                rbuf = tbuf
              end
              buf = nil
              n -= tbuf.size
            when :wait_readable
              nf = now_f
              if expire <= nf
                raise Timeout, "response timeouted"
              else
                _wait_readable(expire - nf)
              end
            when nil
              raise EOFError
            end
          end
          return rbuf
        end
      end

      if defined?(Kgio)
        def _read_nonblock(n, buf)
          return buf ? Kgio.tryread(@socket, n, buf) : Kgio.tryread(@socket, n)
        end
      else
        def _read_nonblock(n, buf)
          begin
            if buf
              @socket.read_nonblock(n, buf)
            else
              @socket.read_nonblock(n)
            end
          rescue IO::WaitReadable
            return :wait_readable
          rescue Errno::EINTR
            retry
          end
        end
      end

      if RUBY_ENGINE == 'jruby'
        def _wait_readable(timeout)
          IO.select([@socket], nil, nil, timeout)
        end
      else
        def _wait_readable(timeout)
          @socket.wait(timeout)
        end
      end
      if IO.instance_methods.include?(:wait_writable)
        def _wait_writable(timeout)
          @socket.wait_writable(timeout)
        end
      else
        def _wait_writable(timeout)
          t = [@socket]
          IO.select(t, t, nil, expire - now_f)
        end
      end
    end
  end
end
