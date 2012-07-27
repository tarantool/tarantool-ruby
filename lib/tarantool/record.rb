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
             when :int, :int64, :varint, :int16, :int8, :sint, :sint64, :sint16, :sint8
               "v = v.to_i  if String === v"
             when :string, :bytes
               ""
             else
               if Serializers::MAP[type]
                 "v = Serializers::MAP[#{type.inspect}].decode(v)  if String === v"
               else
                 raise ArgumentError, "unknown field type #{type.inspect}"
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
          changes = changes()
          if new_record?
            if and_reload
              @attributes = space.insert(@attributes, return_tuple: true)
            else
              space.insert(@attributes)
            end
          else
            return true if changes.size == 0
            ops = []
            changes.each do |k, (old, new)|
              ops << [k.to_sym, :set, new]
            end
            if and_reload
              unless new_attrs = space.update(id, ops, return_tuple: true)
                _raise_doesnt_exists
              end
            else
              if space.update(id, ops) == 0
                _raise_doesnt_exists
              end
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

    # update record in db first, reload updated fileds then
    # (Contrasting with LightRecord, where it reloads all fields)
    # Consider that update operation does not count changes made by
    # attr setters in your code, only field values in DB.
    #
    #   record.update({:state => 'sleep', :sleep_count => [:+, 1]})
    #   record.update([[:state, 'sleep'], [:sleep_count, :+, 1]])
    def update(ops)
      raise UpdateNewRecord, "Could not call update on new record"  if @__new_record
      unless new_attrs = space.update(id, ops, return_tuple: true)
        _raise_doesnt_exists
      end
      for op in ops
        field = op.flatten.first
        @attributes[field] = new_attrs[field]
      end
      self
    end

    def destroy
      run_callbacks :destroy do
        self.class.delete id
        true
      end
    end
  end
end
