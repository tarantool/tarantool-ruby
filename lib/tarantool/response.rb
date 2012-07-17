require 'tarantool/util'
require 'tarantool/exceptions'
require 'tarantool/serializers'

module Tarantool
  module ParseIProto
    include Util::Packer
    def _parse_iproto(data)
      if Exception === data || data == ''
        data
      elsif (ret = unpack_int32!(data)) == 0
        data
      else
        data.gsub!("\x00", "")
        CODE_TO_EXCEPTION[ret].new(ret, data)
      end
    end
  end

  class Response < Struct.new(:cb, :get_tuples, :fields, :translators)
    include Util::Packer
    include Util::TailGetter
    include Serializers
    def call(data)
      if Exception === data
        cb.call(data)
      else
        if (ret = return_code(data)) == 0
          cb.call parse_response(data)
        else
          data.gsub!("\x00", "")
          cb.call CODE_TO_EXCEPTION[ret].new(ret, data)
        end
      end
    end

    def translators
      super || (self.translators = [])
    end

    def call_callback(result)
      cb.call(Exception === result || get_tuples != :first ? result : result.first)
    end

    def parse_response(data)
      unless get_tuples
        unpack_int32(data)
      else
        tuples = unpack_tuples(data)
        if translators
          translators.each{|trans|
            tuples.map!{|tuple| trans.call(tuple)}
          }
        end
        tuples
      end
    end

    def unpack_tuples(data)
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

          tuple << (field_size == 0 ? nil :
            case field
            when :int, :integer
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
            when :str, :string
              tuple_str.slice!(0, field_size).force_encoding('utf-8')
            when :bytes
              tuple_str.slice!(0, field_size)
            when :auto
              str = tuple_str.slice!(0, field_size).force_encoding('utf-8')
              case field_size
              when 8, 4, 2
                Util::AutoType.new(str)
              else
                str
              end
            else
              get_serializer(field).decode(tuple_str.slice!(0, field_size))
            end)
          i += 1
        end
        tuples << tuple
        tuples_affected -= 1
      end
      tuples
    end

    def return_code(data)
      unpack_int32!(data)
    end
  end

  # note that :_tail should not be in field_names
  class TranslateToHash < Struct.new(:field_names, :tail_size)
    def call(tuple)
      i = 0
      hash = {}
      tuple_size = tuple.size
      names = field_names
      while i < tuple_size
        if name = names[i]
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
  end
end
