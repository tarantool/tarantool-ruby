require 'em-tarantool/util'

module EM
  class Tarantool
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

    class ResponseWithTuples < Struct.new(:cb, :fields)
      include Response
      def parse_response(data)
        tuples_affected = unpack_int32!(data)
        tuples = []
        fields = fields()
        while tuples_affected > 0
          byte_size = unpack_int32!(data)
          fields_num = unpack_int32!(data)
          tuple_str = data.slice!(0, byte_size)
          i = 0
          tuple = []
          while i < fields_num
            field_size = unpack_ber!(tuple_str)
            case fields[i]
            when :int
              case field_size
              when 8
                tuple << unpack_int64!(tuple_str)
              when 4
                tuple << unpack_int32!(tuple_str)
              when 2
                tuple << unpack_int16!(tuple_str)
              else
                raise ValueError, "Bad field size #{field_size} for integer field ##{i}"
              end
            else
              tuple << tuple_str.slice!(0, field_size)
            end
            i += 1
          end
          tuples << tuple
          tuples_affected -= 1
        end
        cb.call tuples
      end
    end
  end
end
