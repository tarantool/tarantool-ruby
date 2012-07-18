module Tarantool
  module Serializers
    MAP = {}

    def check_type(type)
      type = type.to_sym  if String === type
      return :varint  if Integer == type
      return :string  if String  == type

      case type
      when :int, :integer
        :int
      when :int64, :integer64
        :int64
      when :varint
        :varint
      when :str, :string
        :string
      when :bytes, :auto
        type
      when Symbol
        unless MAP.include?(type)
          raise ArgumentError, "Unknown type name #{type.inspect}"
        end
        type
      else
        unless type.respond_to?(:encode) && type.respond_to?(:decode)
          raise ArgumentError, "Wrong serializer object #{type.inspect} (must respond to #encode and #decode)"
        end
        type
      end
    end

    def get_serializer(type)
      if Symbol === type
        MAP.fetch(type){ raise ArgumentError, "Unknown type name #{type.inspect}" }
      elsif type.respond_to?(:encode) && type.respond_to?(:decode)
        type
      else
        raise ArgumentError, "Wrong serializer object #{type.inspect} (must respond to #encode and #decode)"
      end
    end

    extend self
  end
end
