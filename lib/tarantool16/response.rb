module Tarantool16
  class Option
    attr :error, :data
    def initialize(err, data)
      @error = err
      @data = data
    end

    def ok?
      !@error
    end

    def raise_if_error!
      raise @error if @error
    end

    def self.ok(data)
      new(nil, data)
    end

    def self.error(err, message = nil)
      if err.is_a? Class
        err = err.new message
      end
      new(err, nil)
    end

    def inspect
      if ok?
        "<Option data=#{@data.inspect}>"
      else
        "<Option error=#{@error.inspect}>"
      end
    end
  end
end
