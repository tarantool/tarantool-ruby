module Tarantool16
  class Option
    def initialize(err, data)
      @error = err
      @data = data
    end
    def ok?
      !@error
    end
    attr :error, :data

    def self.ok(data)
      new(nil, data)
    end

    def self.error(err, message = nil)
      if err.is_a? Class
        err = err.new message
      end
      new(err, nil)
    end
  end
end
