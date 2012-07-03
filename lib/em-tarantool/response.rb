require 'em-tarantool/util'

module EM
  class Tarantool
    class Error < StandardError; end
    class ValueError < Error; end
    class StatusCode < Error
      attr_reader :code
      def initialize(code, msg)
        super(msg)
        @code = code
      end
      def to_s
        "#{super} [#{code}]"
      end
    end
    # try again return codes
    class TryAgain < StatusCode; end
    class TupleReadOnly < TryAgain; end
    class TupleIsLocked < TryAgain; end
    class MemoryIssue   < TryAgain; end
    # general error return codes
    class BadReturnCode < StatusCode; end
    class NonMaster     < BadReturnCode; end
    class IllegalParams < BadReturnCode; end
    class UnsupportedCommand < BadReturnCode; end
    class WrongField    < BadReturnCode; end
    class WrongNumber   < BadReturnCode; end
    class Duplicate     < BadReturnCode; end # it is rather useful
    class WrongVersion  < BadReturnCode; end
    class WalIO         < BadReturnCode; end
    class LuaError      < BadReturnCode; end
    class TupleExists   < BadReturnCode; end
    CODE_TO_EXCEPTION = {
      0x0401 => TupleReadOnly,
      0x0601 => TupleIsLocked,
      0x0701 => MemoryIssue,
      0x0102 => NonMaster,
      0x0202 => IllegalParams,
      0x0a02 => UnsupportedCommand,
      0x1e02 => WrongField,
      0x1f02 => WrongNumber,
      0x2002 => Duplicate,
      0x2602 => WrongVersion,
      0x2702 => WalIO,
      0x3302 => LuaError,
      0x3702 => TupleExists,
    }
    CODE_TO_EXCEPTION.default = BadReturnCode

    module Response
      include EM::Tarantool::Util::Packer

      def call(data)
        if Exception === data
          cb.call(data)
        else
          if (ret = return_code(data)) == 0
            parse_response(data)
          else
            cb.call CODE_TO_EXCEPTION[ret].new(ret, data)
          end
        end
      end

      def return_code(data)
        unpack_int32!(data)
      end
    end

    class ResponseWithoutTuples < Struct.new(:cb)
      include Response
      def parse_response(data)
        cb.call unpack_int32(data)
      end
    end

    class ResponseWithTuples < Struct.new(:cb, :fields, :first)
      include Response
      include Util::TailGetter
      def parse_response(data)
        tuples_affected = unpack_int32!(data)
        tuples = []
        fields = fields()
        if Integer === fields.last
          *fields, tail = fields
        else
          tail = 1
        end

        while tuples_affected > 0
          byte_size = unpack_int32!(data)
          fields_num = unpack_int32!(data)
          tuple_str = data.slice!(0, byte_size)
          i = 0
          tuple = []
          while i < fields_num
            field_size = unpack_ber!(tuple_str)

            field = fields[i] || get_tail_item(fields, i, tail)

            tuple << case field
              when :int, :integer
                case field_size
                when 8
                  unpack_int64!(tuple_str)
                when 4
                  unpack_int32!(tuple_str)
                when 2
                  unpack_int16!(tuple_str)
                when 0
                  nil # well, it is debatable
                else
                  raise ValueError, "Bad field size #{field_size} for integer field ##{i}"
                end
              else
                tuple_str.slice!(0, field_size)
              end
            i += 1
          end
          tuples << tuple
          tuples_affected -= 1
        end
        cb.call first ? tuples[0] : tuples
      end
    end

    class ConvertToHash < Struct.new(:cb, :field_names, :tail_size)
      def map_tuple(tuple, names)
        i = 0
        hash = {}
        tuple_size = tuple.size
        while i < tuple.size
          unless (name = names[i]) == :_tail
            hash[name] = tuple[i]
          else
            tail = tuple.slice(i..-1)
            hash[:_tail] = tail_size == 1 ? tail :
                           tail.each_slice(tail_size).to_a
            break
          end
          i += 1
        end
        hash
      end

      def call(result)
        unless Array === result && !result.empty?
          cb.call(result)
        else
          unless Array === result.first
            cb.call map_tuple(result, field_names)
          else
            field_names = field_names()
            cb.call result.map{|tuple| map_tuple(tuple, field_names)}
          end
        end
      end
    end
  end
end
