require 'tarantool'
require 'em-synchrony'

module Tarantool
  class Space
    alias :deffered_request :request
    def request(*args)
      EM::Synchrony.sync(deffered_request(*args)).tap do |v|
        raise v if v.is_a?(Exception)        
      end
    end
  end
end