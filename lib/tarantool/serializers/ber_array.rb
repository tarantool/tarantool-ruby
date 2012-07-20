module Tarantool
  module Serializers
    class BerArray
      Serializers::MAP[:ber_array] = self
      def self.encode(value)
        value.pack('w*')
      end

      def self.decode(value)
        value.unpack('w*')
      end
    end
  end
end
