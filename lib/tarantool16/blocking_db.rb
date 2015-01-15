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
      cb = RETURN_OR_RAISE
      key = case key
            when nil
              []
            when Array
              key
            when Hash
              cb = lambda{|r|
                raise r.error unless r.ok?
                sp = _space(sno)
                r.data.map{|ar| sp.tuple2hash(ar) }
              }
              key
            else
              [key]
            end
      _select(sno, ino, key, offset, limit, iterator, RETURN_OR_RAISE)
    end

    def insert(sno, tuple)
      _insert(sno, tuple, RETURN_OR_RAISE)
    end

    def replace(sno, tuple)
      _replace(sno, tuple, RETURN_OR_RAISE)
    end

    def delete(sno, key, opts = {})
      ino = opts[:index]
      _delete(sno, ino, key, RETURN_OR_RAISE)
    end

    def update(sno, key, ops, opts = {})
      ino = opts[:index]
      _update(sno, ino, key, ops, RETURN_OR_RAISE)
    end

    def _synchronized
      yield
    end

    class Future < Struct.new(:kind, :cb)
      UNDEF = Object.new.freeze
      def initialize(kind)
        @kind = kind
        @r = UNDEF
        @cb = nil
      end
      def then(cb)
        unless @r.equal? UNDEF
          return cb.call(@r)
        end
        raise "Blocking future accepts unly 1 callback" if @cb
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
