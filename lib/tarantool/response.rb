require 'tarantool/util'
require 'tarantool/exceptions'
require 'tarantool/serializers'

module Tarantool
  module UnpackTuples
    def _unpack_field(tuple_str, field, i, realfield, serializers)
      field_size = ::BinUtils.slice_ber!(tuple_str)
      return nil if field_size == 0

      case field
      when :int, :integer
        if field_size != 4
          raise ValueError, "Bad field size #{field_size} for integer field ##{i}"
        end
        ::BinUtils.slice_int32_le!(tuple_str)
      when :string, :str
        str = tuple_str.slice!(0, field_size)
        str[0,1] = EMPTY  if str < ONE
        str.force_encoding(UTF8)
      when :int64
        if field_size != 8
          raise ValueError, "Bad field size #{field_size} for 64bit integer field ##{i}"
        end
        ::BinUtils.slice_int64_le!(tuple_str)
      when :bytes
        tuple_str.slice!(0, field_size)
      when :int16
        if field_size != 2
          raise ValueError, "Bad field size #{field_size} for 16bit integer field ##{i}"
        end
        ::BinUtils.slice_int16_le!(tuple_str)
      when :int8
        if field_size != 1
          raise ValueError, "Bad field size #{field_size} for 8bit integer field ##{i}"
        end
        ::BinUtils.slice_int8!(tuple_str)
      when :sint
        if field_size != 4
          raise ValueError, "Bad field size #{field_size} for integer field ##{i}"
        end
        ::BinUtils.slice_sint32_le!(tuple_str)
      when :sint64
        if field_size != 8
          raise ValueError, "Bad field size #{field_size} for 64bit integer field ##{i}"
        end
        ::BinUtils.slice_sint64_le!(tuple_str)
      when :sint16
        if field_size != 2
          raise ValueError, "Bad field size #{field_size} for 16bit integer field ##{i}"
        end
        ::BinUtils.slice_sint16_le!(tuple_str)
      when :sint8
        if field_size != 1
          raise ValueError, "Bad field size #{field_size} for 8bit integer field ##{i}"
        end
        ::BinUtils.slice_sint8!(tuple_str)
      when :varint
        case field_size
        when 8
          ::BinUtils.slice_int64_le!(tuple_str)
        when 4
          ::BinUtils.slice_int32_le!(tuple_str)
        when 2
          ::BinUtils.slice_int16_le!(tuple_str)
        else
          raise ValueError, "Bad field size #{field_size} for integer field ##{i}"
        end
      when :auto
        str = tuple_str.slice!(0, field_size).force_encoding('utf-8')
        case field_size
        when 8, 4, 2
          Util::AutoType.new(str)
        else
          str
        end
      else
        (serializers[realfield] ||= get_serializer(field)).decode(tuple_str.slice!(0, field_size))
      end
    end
  end

  module ParseIProto
    include Util::Packer
    def _parse_iproto(data)
      if Exception === data || data == ''
        data
      elsif (ret = ::BinUtils.slice_int32_le!(data)) == 0
        data
      else
        data.gsub!("\x00", "")
        CODE_TO_EXCEPTION[ret].new(ret, data)
      end
    end
  end

  class Response < Struct.new(:cb, :request_type, :body, :get_tuples, :fields, :translators)
    include Util::Packer
    include Util::TailGetter
    include Serializers
    include UnpackTuples
    UTF8 = 'utf-8'.freeze

    def call(data)
      if Exception === data
        cb.call(data)
      else
        if (ret = return_code(data)) == 0
          call_callback parse_response_for_cb(data)
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

    def parse_response_for_cb(data)
      parse_response data
    rescue StandardError => e
      e
    end

    def parse_response(data)
      return data  if Exception === data
      unless get_tuples
        ::BinUtils.get_int32_le(data)
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
      tuples_affected = ::BinUtils.slice_int32_le!(data)
      ta = tuples_affected
      fields = fields()
      if Integer === fields.last
        *fields, tail = fields
      else
        tail = 1
      end
      orig_data = data.dup
      begin
        tuples = []
        serializers = []
        while tuples_affected > 0
          byte_size = ::BinUtils.slice_int32_le!(data)
          fields_num = ::BinUtils.slice_int32_le!(data)
          tuple_str = data.slice!(0, byte_size)
          i = 0
          tuple = []
          while i < fields_num
            field = fields[fieldno = i] || fields[fieldno = get_tail_no(fields, i, tail)]
            tuple << _unpack_field(tuple_str, field, i, fieldno, serializers)
            i += 1
          end
          tuples << tuple
          tuples_affected -= 1
        end
        tuples
      rescue ValueError => e
        $stderr.puts "Value Error: tuples=#{ta} now=#{ta-tuples_affected}, remains=#{data.bytesize} remains_data='#{data.unpack('H*')[0].gsub(/../,'\& ')}' orig_size=#{orig_data.size} orig_data='#{orig_data.unpack('H*')[0].gsub(/../,'\& ')}'"
        raise e
      end
    end

    def return_code(data)
      ::BinUtils.slice_int32_le!(data)
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
