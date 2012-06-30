require 'em-tarantool/util'

module EM
  class Tarantool
    class Error < StandardError; end
    class ValueError < Error; end
    class StatusCode < Error; end
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
      0x2702 => WalIO
    }

    module Response
      include EM::Tarantool::Util::Packer

      def call(data)
        if Exception === data
          cb.call(data)
        else
          if (ret = return_code(data)) == 0
            parse_response(data)
          else
            cb.call BadReturnCode.new("Error: #{ret} #{data}")
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
              when :int
                case field_size
                when 8
                  unpack_int64!(tuple_str)
                when 4
                  unpack_int32!(tuple_str)
                when 2
                  unpack_int16!(tuple_str)
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
  end
end
