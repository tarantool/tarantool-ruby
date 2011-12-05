module Tarantool
  module Serializers
    MAP = {}
    %w{string integer}.each do |v|
      require "tarantool/serializers/#{v}"
    end

  end
end