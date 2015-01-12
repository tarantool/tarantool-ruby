require "tarantool16/version"
require "tarantool16/db"

module Tarantool16
  autoload :BlockingDB, 'tarantool16/blocking_db'
  def self.new(opts = {})
    opts = opts.dup
    hosts = opts[:host]
    type = opts[:type] && opts[:type].to_s || 'blocking'
    case type
    when 'blocking'
      BlockingDB.new hosts, opts
    else
      raise "Unknown DB type"
    end
  end
end
