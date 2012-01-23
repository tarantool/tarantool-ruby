class Tarantool
  require 'tarantool/request'
  module Requests
    REQUEST_TYPES = {
      insert: 13,
      select: 17,
      update: 19,
      delete: 21,
      call: 22,
      ping:  65280
    }
    BOX_RETURN_TUPLE = 1
    BOX_ADD = 2

    %w{insert select update delete call ping}.each do |v|
      require "tarantool/requests/#{v}"
    end
  end
end