require 'bson'
module Tarantool
  module Serializers
    class BSON
      Serializers::MAP[:bson] = self
      def self.encode(value)
        ::BSON.serialize(value).to_s
      end

      def self.decode(field)
        ::BSON.deserialize(field.to_s)
      end
    end
  end
end
