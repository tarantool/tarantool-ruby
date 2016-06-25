require "tarantool16/version"
require "tarantool16/db"

module Tarantool16
  autoload :DumbDB, 'tarantool16/dumb_db'
  def self.new(opts = {})
    opts = opts.dup
    if opts[:unix]
      raise "`:host` and `:unix` options are mutually exclusive"
      opts[:host] = "unix:#{opts.delete(:unix)}"
    end
    hosts = [opts[:host], opts[:port]].compact.join(':')
    type = opts[:type] && opts[:type].to_s || 'dumb'
    case type
    when 'dumb'
      DumbDB.new hosts, opts
    else
      raise "Unknown DB type"
    end
  end
end
