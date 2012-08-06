require 'bin_utils'
module Tarantool
  module Util
    module Packer
      INT8  = 'C'.freeze
      INT16 = 'v'.freeze
      INT32 = 'V'.freeze
      INT64 = 'Q<'.freeze
      SINT8  = 'c'.freeze
      SINT16 = 's<'.freeze
      SINT32 = 'l<'.freeze
      SINT64 = 'q<'.freeze
      MIN_INT   = 0
      MAX_INT64 = 2**64 - 1
      MAX_INT32 = 2**32 - 1
      MAX_INT16 = 2**16 - 1
      MAX_INT8 = 2**8 - 1
      MAX_SINT64 = 2**63 - 1
      MAX_SINT32 = 2**31 - 1
      MAX_SINT16 = 2**15 - 1
      MAX_SINT8 = 2**7 - 1
      MIN_SINT64 = -(2**63)
      MIN_SINT32 = -(2**31)
      MIN_SINT16 = -(2**15)
      MIN_SINT8  = -(2**7)
    private
      EMPTY = ''.freeze
      ONE = "\x01".freeze
      def ber_size(int)
        int < 128 ? 1 :
        int < 16384 ? 2 :
        int < 2097153 ? 3 :
        int < 268435456 ? 4 : 5
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

    module Array
      def frozen_array(obj)
        (Array === obj ? obj.dup : [*obj]).freeze
      end
    end

    class AutoType
      include Packer
      include Comparable

      attr_reader :data
      def initialize(data)
        @data = data
      end

      def to_int
        case @data.bytesize
        when 8
          ::BinUtils.get_int64_le(@data)
        when 4
          ::BinUtils.get_int32_le(@data)
        when 2
          ::BinUtils.get_int16_le(@data)
        else
          raise ValueError, "Bad field size #{field_size} for integer field ##{i}"
        end
      end
      alias to_i to_int

      def coerce(oth)
        case oth
        when Numeric
          [oth, to_i]
        when String
          [oth, @data]
        end
      end

      alias to_str data
      alias to_s data
      def inspect
        "<#{self.class.name} data=#{@data.inspect}>"
      end

      def ==(oth)
        case oth
        when Numeric
          to_i == oth
        when String
          @data == oth
        when AutoType
          @data == oth.data
        end
      end
      alias eql? ==

      def <=>(oth)
        case oth
        when Numeric
          to_i <=> oth
        when String
          @data <=> oth
        when AutoType
          @data <=> oth.data
        end
      end

      def +(oth)
        case oth
        when Numeric
          to_i + oth
        when String
          @data + oth
        when AutoType
          @data + oth.data
        end
      end

      def -(oth) to_i - oth end
      def *(oth) to_i * oth end
      def /(oth) to_i / oth end
      def %(oth) to_i % oth end
      def **(oth) to_i ** oth end

      def empty?;   @data.empty?    end
      def bytesize; @data.bytesize  end
      def size;     @data.size      end
      def length;   @data.length    end
      def hash;     @data.hash      end
      %w{[] sub gsub slice =~ match scan split upcase downcase bytes each_byte
        byteslice capitalize casecmp center chars each_char chomp chop chr
        codepoints each_codepoint count crypt delete dump each_line lines
        empty? encode end_with? getbyte hex include? index intern to_sym
        ljust lstrip succ next oct ord partition reverse rindex
        rjust rpartition rstrip squeeze start_with? strip swapcase to_c to_f
        to_r intern to_sym tr unpack upto}.each do |meth|
        class_eval <<-EOF, __FILE__, __LINE__
          def #{meth}(*args, &block)
            @data.#{meth}(*args, &block)
          end
        EOF
      end
    end
  end
end
