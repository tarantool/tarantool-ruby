class Tarantool
  module Util
    module Packer
    private
      EMPTY = ''.freeze
      def unpack_int16!(data)
        int = data.getbyte(0) + data.getbyte(1) * 256
        data[0, 2] = EMPTY
        data
      end

      def unpack_int32(int)
        (int.getbyte(0) + int.getbyte(1) * 256 +
         int.getbyte(2) * 65536 + int.getbyte(3) * 16777216)
      end

      def unpack_int32!(data)
        int = (data.getbyte(0) + data.getbyte(1) * 256 +
               data.getbyte(2) * 65536 + data.getbyte(3) * 16777216)
        data[0, 4] = EMPTY
        int
      end

      def unpack_int64!(data)
        int = (data.getbyte(0) + data.getbyte(1) * 256 +
               data.getbyte(2) * 65536 + data.getbyte(3) * 16777216 +
               data.getbyte(4) << 32 + data.getbyte(5) << 40 +
               data.getbyte(6) << 48 + data.getbyte(7) << 56
              )
        data[0, 8] = EMPTY
        int
      end

      def ber_size(int)
        int < 128 ? 1 :
        int < 16384 ? 2 :
        int < 2097153 ? 3 :
        int < 268435456 ? 4 : 5
      end

      def unpack_ber!(data)
        res = 0
        pos = 0
        while true
          if (byte = data.getbyte(pos)) <= 127
            res += byte
            break
          else
            res = (res + (byte - 128)) * 128
            pos += 1
          end
        end
        data[0, pos+1] = EMPTY
        res
      end
    end

    module TailGetter
      private
      def get_tail_item(array, index, tail)
        tail == 1 ?
          array.last :
          array[array.size - tail + (index - array.size) % tail]
      end
    end
  end
end
