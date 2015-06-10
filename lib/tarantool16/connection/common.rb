require 'msgpack'
require 'openssl'
require 'openssl/digest'
require 'tarantool16/consts'
require_relative 'response'

module Tarantool16
  module Connection
    class Error < ::StandardError; end
    class ConnectionError < Error; end
    class CouldNotConnect < ConnectionError; end
    class Disconnected < ConnectionError; end
    class Retry < ConnectionError; end
    class UnexpectedResponse < Error; end

    module Common
      DEFAULT_RECONNECT = 0.2
      attr :host, :user
      def _init_common(host, opts)
        @host = host
        @user = opts[:user]
        if opts[:password]
          @passwd = ::OpenSSL::Digest::SHA1.digest(opts[:password])
        end
        if opts[:reconnect].nil?
          @reconnect_timeout = DEFAULT_RECONNECT
          @reconnect = true
        elsif Numeric === opts[:reconnect]
          @reconnect_timeout = opts[:reconnect]
          @reconnect = true
        else
          @reconnect = false
        end
        @p = MessagePack::Packer.new
        @u = MessagePack::Unpacker.new
        @s = 0
      end

      if ::Process.respond_to?(:clock_gettime)
        if defined?(::Process::CLOCK_MONOTONIC_COARSE)
          CLOCK_KIND = ::Process::CLOCK_MONOTONIC_COARSE
        elsif defined?(::Process::CLOCK_MONOTONIC_FAST)
          CLOCK_KIND = ::Process::CLOCK_MONOTONIC_FAST
        elsif defined?(::Process::CLOCK_MONOTONIC)
          CLOCK_KIND = ::Process::CLOCK_MONOTONIC
        else
          CLOCK_KIND = ::Process::CLOCK_REALTIME
        end
        def now_f
          ::Process.clock_gettime(CLOCK_KIND)
        end
      else
        def now_f
          Time.now.to_f
        end
      end

      def next_sync
        @s = @s % 0x3fffffff + 1
      end

      def format_request(code, sync, body)
        @p.write(0x01020304).
          write_map_header(2).
          write(IPROTO_CODE).write(code).
          write(IPROTO_SYNC).write(sync).
          write(body)
        sz = @p.size - 5
        str = @p.to_s
        @p.clear
        # fix bigendian size
        str.setbyte(4, sz)
        str.setbyte(3, sz>>8)
        str.setbyte(2, sz>>16)
        str.setbyte(1, sz>>24)
        str
      end

      def format_authenticate(user, pass1, salt)
        pass2 = ::OpenSSL::Digest::SHA1.digest(pass1)
        scramble = ::OpenSSL::Digest::SHA1.new(salt).update(pass2).digest
        pints = pass1.unpack('L*')
        sints = scramble.unpack('L*')
        pints.size.times{|i| sints[i] ^= pints[i] }
        packed = sints.pack('L*')
        # tarantool waits packed as a string, so that force msgpack to pack as string
        packed.force_encoding('utf-8')
        format_request(REQUEST_TYPE_AUTHENTICATE, next_sync, {
          IPROTO_USER_NAME => user,
          IPROTO_TUPLE => [ 'chap-sha1', packed ]
        })
      end

      def parse_greeting(greeting)
        @greeting = greeting[0, 64]
        @salt = greeting[64..-1].unpack('m')[0][0,20]
      end

      def parse_size(str)
        @u.feed(str)
        n = @u.read
        unless Integer === n
          return UnexpectedResponse.new("wanted response size, got #{n.inspect}")
        end
        n
      rescue ::MessagePack::UnpackError, ::MessagePack::TypeError => e
        e
      end

      def parse_response(str)
        sync = nil
        @u.feed(str)
        n = @u.read_map_header
        while n > 0
          cd = @u.read
          vl = @u.read
          case cd
          when IPROTO_SYNC
            sync = vl
          when IPROTO_CODE
            code = vl
          end
          n -= 1
        end
        if sync == nil
          return Option.error(nil, UnexpectedResponse, "Mailformed response: no sync")
        elsif code == nil
          return Option.error(nil, UnexpectedResponse, "Mailformed response: no code for sync=#{sync}")
        end
        unless @u.buffer.empty?
          bmap = @u.read
          body = bmap[IPROTO_DATA] || bmap[IPROTO_ERROR]
        else
          body = nil
        end
        Option.ok(sync, code, body)
      rescue ::MessagePack::UnpackError, ::MessagePack::TypeError => e
        Option.error(sync, e, nil)
      end

      def host_port
        h, p = @host.split(':')
        [h, p.to_i]
      end

      def _insert(space_no, tuple, cb)
        req = {IPROTO_SPACE_ID => space_no,
               IPROTO_TUPLE => tuple}
        send_request(REQUEST_TYPE_INSERT, req, cb)
      end

      def _replace(space_no, tuple, cb)
        req = {IPROTO_SPACE_ID => space_no,
               IPROTO_TUPLE => tuple}
        send_request(REQUEST_TYPE_REPLACE, req, cb)
      end

      def _delete(space_no, index_no, key, cb)
        req = {IPROTO_SPACE_ID => space_no,
               IPROTO_INDEX_ID => index_no,
               IPROTO_KEY => key}
        send_request(REQUEST_TYPE_DELETE, req, cb)
      end

      def _select(space_no, index_no, key, offset, limit, iterator, cb)
        iterator ||= ::Tarantool16::ITERATOR_EQ
        unless Integer === iterator
          iterator = ::Tarantool16.iter(iterator)
        end
        req = {IPROTO_SPACE_ID => space_no,
               IPROTO_INDEX_ID => index_no,
               IPROTO_KEY => key || [],
               IPROTO_OFFSET => offset,
               IPROTO_LIMIT => limit,
               IPROTO_ITERATOR => iterator}
        send_request(REQUEST_TYPE_SELECT, req, cb)
      end

      def _update(space_no, index_no, key, ops, cb)
        req = {IPROTO_SPACE_ID => space_no,
               IPROTO_INDEX_ID => index_no,
               IPROTO_KEY => key,
               IPROTO_TUPLE => ops}
        send_request(REQUEST_TYPE_UPDATE, req, cb)
      end

      def _call(name, args, cb)
        req = {IPROTO_FUNCTION_NAME => name,
               IPROTO_TUPLE => args}
        send_request(REQUEST_TYPE_CALL, req, cb)
      end

      REQ_EMPTY = {}.freeze
      def _ping(cb)
        send_request(REQUEST_TYPE_PING, REQ_EMPTY, cb)
      end
    end
  end
end
