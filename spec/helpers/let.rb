module Helpers
  module Let
    def let(name, &blk)      
      define_method name do
        @let_assigments ||= {}
        @let_assigments[name] ||= send(:"original_#{name}")
      end
      define_method "original_#{name}", &blk
    end
  end
end