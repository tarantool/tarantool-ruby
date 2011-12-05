module Tarantool
  module Serializers
    class Integer
      Serializers::MAP[:integer] = self
      def self.encode(value)
        value.to_i
      end

      def self.decode(field)
        field.to_i
      end
    end
  end
end