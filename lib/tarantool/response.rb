class Tarantool
  class Field
    attr_reader :data
    def initialize(data)
      @data = data
    end

    def to_i
      @i ||= case data.bytesize
      when 8
        data.unpack('Q<')[0]
      when 4
        data.unpack('V')[0]
      when 2
        data.unpack('S')[0]
      else
        raise ValueError.new("Unable to cast field to int: length must be 2, 4 or 8 bytes, field length is #{data.size}")
      end
    end

    def to_s
      @s ||= data.dup.force_encoding('utf-8')
    end
  end
  class Response
    attr_reader :tuples_affected, :offset, :tuples
    def initialize(data, params = {})
      @offset = 0
      @tuples_affected, = data[0, 4].unpack('V')
      @offset += 4
      if params[:return_tuple]
        @tuples = (1..tuples_affected).map do
          unpack_tuple(data)
        end
      else
        tuples_affected
      end
    end

    # Only select request can return many tuples
    def tuple
      tuples.first
    end

    def unpack_tuple(data)
      byte_size, cardinality = data[@offset, 8].unpack("VV")
      @offset += 8
      tuple_data = data[@offset, byte_size]
      @offset += byte_size
      (1..cardinality).map do
        Field.new unpack_field(tuple_data)
      end
    end

    EMPTY = ''.freeze
    def unpack_field(data)
      byte_size,  = data.unpack('w')
      rem = byte_size < 128 ? 1 :
            byte_size < 16384 ? 2 :
            byte_size < 2097152 ? 3 :
            byte_size < 268435456 ? 4 :
            5 # Assume, we do not store values longer than 32GB
      data[0, rem] = EMPTY
      data.slice!(0, byte_size)
    end
  end
end
