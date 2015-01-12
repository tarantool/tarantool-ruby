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
      case key
      when nil
        []
      when Array
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


  end
end
