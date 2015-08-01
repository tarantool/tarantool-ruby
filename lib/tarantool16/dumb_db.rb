require_relative 'db'
require_relative 'connection/dumb'

module Tarantool16
  class DumbDB < DB
    Connection = Tarantool16::Connection::Dumb

    RETURN_OR_RAISE = lambda{|r|
      raise r.error unless r.ok?
      r.data
    }
    RETURN_ONE_OR_RAISE = lambda{|r|
      raise r.error unless r.ok?
      r.data[0]
    }
    HUGE_LIMIT = 2**30

    def select(sno, key, opts={})
      ino = opts[:index]
      offset = opts[:offset] || 0
      limit = opts[:limit] || 2**30
      iterator = opts[:iterator]
      need_hash = opts.fetch(:hash, Hash === key)
      key = _norm_key(key)
      _select(sno, ino, key, offset, limit, iterator, need_hash, RETURN_OR_RAISE)
    end

    def get(sno, key, opts={})
      ino = opts[:index]
      iterator = opts[:iterator]
      need_hash = opts.fetch(:hash, Hash === key)
      key = _norm_key(key)
      _select(sno, ino, key, 0, 1, iterator, need_hash, RETURN_ONE_OR_RAISE)
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
      key_hash = Hash === key
      ino = opts[:index] || (key_hash ? nil : 0)
      need_hash = opts.fetch(:hash, key_hash)
      key = _norm_key(key)
      _delete(sno, ino, key, need_hash, RETURN_OR_RAISE)
    end

    def update(sno, key, ops, opts = {})
      key_hash = Hash === key
      ino = opts[:index] || (key_hash ? nil : 0)
      need_hash = opts.fetch(:hash, key_hash)
      key = _norm_key(key)
      _update(sno, ino, key, ops, need_hash, RETURN_OR_RAISE)
    end

    def call(name, args)
      _call(name, args, RETURN_OR_RAISE)
    end

    def _synchronized
      yield
    end

    def _norm_key(key)
      case key
      when Array
        key
      when Hash
        key
      else
        [key]
      end
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

      def then_blk
        unless @r.equal? UNDEF
          return yield @r
        end
        if @cb
          raise "Blocking future accepts only 1 callback"
        end
        @cb = lambda{|r| yield r}
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
