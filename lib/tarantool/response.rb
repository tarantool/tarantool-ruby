require 'tarantool/util'
require 'tarantool/exceptions'

module Tarantool
  class Response < Struct.new(:cb, :get_tuples, :fields, :translators)
    include Util::Packer
    include Util::TailGetter
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
        get_tuples == :first ? tuples.first : tuples
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
            when :str, :string
              tuple_str.slice!(0, field_size).force_encoding('utf-8')
            when :bytes
              typle_str.slice!(0, field_size)
            else
              if serializer = field.respond_to?(:decode) ? field :
                              Tarantool::Serializers::MAP[field]
                serializer.decode(tuple_str.slice!(0, field_size))
              else
                raise ArgumentError, "Unknown field type #{field.inspect}"
              end
            end
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

  class TranslateToHash < Struct.new(:field_names, :tail_size)
    def call(tuple)
      i = 0
      hash = {}
      tuple_size = tuple.size
      names = field_names
      while i < tuple_size
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
  end
end
