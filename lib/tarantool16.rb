require "tarantool16/version"
require "tarantool16/db"

module Tarantool16
  autoload :DumbDB, 'tarantool16/dumb_db'
  def self.new(opts = {})
    opts = opts.dup
    if opts[:unix] && opts[:host]
        raise "`:host` and `:unix` options are mutually exclusive"
    elsif opts[:unix]
      hosts = ["unix", opts[:unix]]
    elsif opts[:host]
      host = opts[:host]
      if Array === host
        hosts = host
      else
        host = [host, opts[:port]].compact.join(':')
        hosts = ["tcp", host]
      end
    end
    type = opts[:type] && opts[:type].to_s || 'dumb'
    case type
    when 'dumb'
      DumbDB.new hosts, opts
    else
      raise "Unknown DB type"
    end
  end
end
