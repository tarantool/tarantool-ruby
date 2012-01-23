class Tarantool
  module Serializers
    class String
      Serializers::MAP[:string] = self
      def self.encode(value)
        value.to_s
      end

      def self.decode(field)
        field.to_s
      end
    end
  end
end