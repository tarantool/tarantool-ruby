require 'active_model'
require 'tarantool/base_record'

module Tarantool
  class Record < BaseRecord
    extend ActiveModel::Naming
    include ActiveModel::AttributeMethods
    include ActiveModel::Validations
    include ActiveModel::Serialization
    extend ActiveModel::Callbacks
    include ActiveModel::Dirty

    include ActiveModel::Serializers::JSON
    include ActiveModel::Serializers::Xml

    define_model_callbacks :save, :create, :update, :destroy
    define_model_callbacks :initialize, :only => :after

    class << self
      def set_space_no(val)
        self.space_no = val
      end

      def set_tarantool(val)
        case val.class.name
        when 'Tarantool::BlockDB', 'Tarantool::FiberDB'
          self.tarantool = val
        else
          raise "Tarantool should be blocking of fibered!!! (i.e. of class Tarantool::BlockDB or Tarantool::FiberDB) (got #{val.class})"
        end
      end

      def define_field_accessor(name, type)
        generated_attribute_methods.class_eval <<-"EOF", __FILE__, __LINE__ - 1
          def #{name}
            @attributes[:"#{name}"]
          end
        EOF

        if Symbol === type
          convert_code = case type
             when :int, :integer
               "v = v.to_i  if String === v"
             when :str, :string
               ""
             else
               if serializer = Serializers::MAP[type]
                 "v = Serializers::MAP[#{type.inspect}].decode(v)  if String === v"
               else
                 raise ValueError, "unknown field type #{type.inspect}"
               end
             end

          generated_attribute_methods.class_eval <<-"EOF", __FILE__, __LINE__ - 1
            def #{name}=(v)
              #{convert_code}
              #{name}_will_change!  unless v == @attributes[:"#{name}"] || new_record?
              @attributes[:"#{name}"] = v
            end
          EOF
        else
          generated_attribute_methods.class_eval do
            define_method("#{name}=") do |v|
              v = type.decode(v)  if String === v
              send(:"#{name}_will_change!") unless v == @attributes[name]
              @attributes[name] = v
            end
          end
        end
        define_attribute_method name
      end
    end

    def initialize(attributes = {})
      @__new_record = true
      @attributes = self.class.default_values.dup
      run_callbacks(:initialize) do
        init attributes
      end
    end

    def init(attributes)
      set_attributes(attributes)
    end

    def __fetched(attributes)
      @__new_record = false
      # well, initalize callback could call #attributes
      @attributes = self.class.default_values.dup
      run_callbacks(:initialize) do
        @attributes = attributes
      end
      self
    end

    def _in_callbacks(&blk)
      run_callbacks(:save) {
        run_callbacks(new_record? ? :create : :update, &blk)
      }
    end

    def save(and_reload = true)
      _in_callbacks do
        if valid?
          if new_record?
            if and_reload
              @attributes = space.insert(@attributes, return_tuple: true)
            else
              space.insert(@attributes)
            end
          else
            return true if changed.size == 0
            ops = {}
            changed.each do |k|
              k = k.to_sym
              ops[k] = attributes[k]
            end
            if and_reload
              @attributes = space.update id, ops, return_tuple: true
            else
              space.update id, ops
            end
          end
          @previously_changed = changes
          @changed_attributes.clear
          old_record!
          true
        else
          false
        end
      end
    end

    def destroy
      run_callbacks :destroy do
        self.class.delete id
        true
      end
    end
  end
end
