require 'iproto/core-ext'

module Tarantool
  module ClassAttribute
    # spinoff from ActiveSupport class attribute
    def t_class_attribute(*attrs)
      attrs.each do |name|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def self.#{name}() nil end
          def self.#{name}?() !!#{name} end

          def self.#{name}=(val)
            singleton_class.class_eval do
              begin
                if method_defined?(:"#{name}") || private_method_defined?(:"#{name}")
                  remove_method(:#{name})
                end
              rescue NameError
              end
              define_method(:#{name}) { val }
            end
            val
          end
        RUBY
      end
    end
  end
end
