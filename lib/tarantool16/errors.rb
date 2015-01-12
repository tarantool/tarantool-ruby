
module Tarantool16
  class Error < ::StandardError; end
  class ConnectionError < Error; end
  DBErrors = {}
  class SchemaError < Error; end
  class DBError < Error
    class KnownDBError < DBError
      class << self
        attr_accessor :return_code
      end
      def return_code
        self.class.return_code
      end
    end
    class UnknownDBError < DBError
      attr_accessor :return_code
    end

    {
       1=> :ER_ILLEGAL_PARAMS,
       2=> :ER_MEMORY_ISSUE,
       3=> :ER_TUPLE_FOUND,
       4=> :ER_TUPLE_NOT_FOUND,
       5=> :ER_UNSUPPORTED,
       6=> :ER_NONMASTER,
       7=> :ER_SECONDARY,
       8=> :ER_INJECTION,
       9=> :ER_CREATE_SPACE,
      10=> :ER_SPACE_EXISTS,
      11=> :ER_DROP_SPACE,
      12=> :ER_ALTER_SPACE,
      13=> :ER_INDEX_TYPE,
      14=> :ER_MODIFY_INDEX,
      15=> :ER_LAST_DROP,
      16=> :ER_TUPLE_FORMAT_LIMIT,
      17=> :ER_DROP_PRIMARY_KEY,
      18=> :ER_KEY_FIELD_TYPE,
      19=> :ER_EXACT_MATCH,
      20=> :ER_INVALID_MSGPACK,
      21=> :ER_PROC_RET,
      22=> :ER_TUPLE_NOT_ARRAY,
      23=> :ER_FIELD_TYPE,
      24=> :ER_FIELD_TYPE_MISMATCH,
      25=> :ER_SPLICE,
      26=> :ER_ARG_TYPE,
      27=> :ER_TUPLE_IS_TOO_LONG,
      28=> :ER_UNKNOWN_UPDATE_OP,
      29=> :ER_UPDATE_FIELD,
      30=> :ER_FIBER_STACK,
      31=> :ER_KEY_PART_COUNT,
      32=> :ER_PROC_LUA,
      33=> :ER_NO_SUCH_PROC,
      34=> :ER_NO_SUCH_TRIGGER,
      35=> :ER_NO_SUCH_INDEX,
      36=> :ER_NO_SUCH_SPACE,
      37=> :ER_NO_SUCH_FIELD,
      38=> :ER_SPACE_ARITY,
      39=> :ER_INDEX_ARITY,
      40=> :ER_WAL_IO,
      41=> :ER_MORE_THAN_ONE_TUPLE,
    }.each do |n, s|
      klass = Class.new(KnownDBError)
      klass.return_code = n
      Tarantool16::DBErrors[n] = klass
      Tarantool16.const_set(s, klass)
    end
    def self.with_code_message(n, m="")
      if klass = DBErrors[n]
        klass.new(m)
      else
        e = UnknownDBError.new(m)
        e.return_code = n
        e
      end
    end

    def inspect
      "<#{self.class.name} return_code=#{return_code} message=#{message}>"
    end
  end
end
