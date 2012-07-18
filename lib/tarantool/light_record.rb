require 'tarantool/base_record'

module Tarantool
  class LightRecord < BaseRecord
    def initialize(attributes = nil)
      @__new_record = true
      @attributes = self.class.default_values.dup
      set_attributes(attributes)  if attributes
      after_init
    end

    def __fetched(attributes)
      @__new_record = false
      @attributes = attributes
      after_init
      self
    end

    # callback which runs both after initialization and after
    # fetching from database
    def after_init
    end

    def save
      return false  unless before_save
      if @__new_record
        return false  unless before_create
        self.class.insert(@attributes)
        @__new_record = false
        after_create
      else
        return false  unless before_update
        self.class.replace(@attributes)
        after_update
      end
      after_save
      self
    end

    def destroy
      return false  unless before_destroy
      self.class.delete id
      after_destroy
      true
    end

    def before_save; true end
    def before_create; true end
    def before_update; true end
    def after_create; end
    def after_update; end
    def after_save; end
    def before_destroy; true end
    def after_destroy; end

    class << self
      def generated_attribute_methods
        @generated_attribute_methods ||= begin
            include (mod = Module.new)
            mod
          end
      end

      def define_field_accessor(name, type)
        generated_attribute_methods.class_eval <<-"EOF", __FILE__, __LINE__ - 1
          def #{name}
            @attributes[:"#{name}"]
          end

          def #{name}=(v)
            @attributes[:"#{name}"] = v
          end
        EOF
      end

      def create(attrs)
        new(attrs).save
      end

      def from_fetched(attributes)
        attributes && allocate.__fetched(attributes)
      end
    end
  end
end
