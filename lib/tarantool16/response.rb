module Tarantool16
  class ResponseError
    def initialize(err, message = nil)
      if StandardError === err
        @error = err
      else
        @error = err.new message
      end
    end

    def ok?
      false
    end

    attr :error

    def data
      nil
    end
  end

  class ResponseData
    def initialize(data)
      @data = data
    end

    def ok?
      true
    end

    attr :data

    def error
      nil
    end
  end
end
