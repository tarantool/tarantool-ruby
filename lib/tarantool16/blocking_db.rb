require_relative 'db'
require_relative 'connection/blocking'

module Tarantool16
  class BlockingDB < DB
    Connection = Tarantool16::Connection::Blocking

    RETURN_OR_RAISE = lambda{|r|
      raise r.error unless r.ok?
      r.data
    }
    HUGE_LIMIT = 2**30

    def select(sno, key, opts={})
      ino = opts[:index]
      offset = opts[:offset] || 0
      limit = opts[:limit] || 2**30
      iterator = opts[:iterator]
      need_hash = opts[:hash]
      key = case key
            when nil
              []
            when Array
              key
            when Hash
              need_hash = true
              key
            else
              [key]
            end
      _select(sno, ino, key, offset, limit, iterator, need_hash, RETURN_OR_RAISE)
    end

    def insert(sno, tuple, opts = {})
      need_hash = opts[:hash] || tuple.is_a?(Hash)
      _insert(sno, tuple, need_hash, RETURN_OR_RAISE)
    end

    def replace(sno, tuple, opts = {})
      need_hash = opts[:hash] || tuple.is_a?(Hash)
      _replace(sno, tuple, need_hash, RETURN_OR_RAISE)
    end

    def delete(sno, key, opts = {})
      ino = opts[:index]
      need_hash = opts[:hash] || tkey.is_a?(Hash)
      _delete(sno, ino, key, need_hash, RETURN_OR_RAISE)
    end

    def update(sno, key, ops, opts = {})
      ino = opts[:index]
      need_hash = opts[:hash] || key.is_a?(Hash)
      _update(sno, ino, key, ops, need_hash, RETURN_OR_RAISE)
    end

    def _synchronized
      yield
    end

    class SchemaFuture
      UNDEF = Object.new.freeze
      def initialize
        @r = UNDEF
        @cb = nil
      end
      def then(cb)
        unless @r.equal? UNDEF
          return cb.call(@r)
        end
        if @cb
          raise "Blocking future accepts only 1 callback"
        end
        @cb = cb
      end

      def set(r)
        @r = r
        if cb = @cb
          @cb = nil
          cb.call(r)
        end
      end
    end

  end
end
