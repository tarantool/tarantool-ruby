require 'tarantool16/errors'
require 'tarantool16/response'

module Tarantool16
  module Connection
    class Option
      attr :sync, :error, :data
      def initialize(sync, err, data)
        @sync = sync
        @error = err
        @data = data
      end

      def ok?
        !@error
      end

      def raise_if_error!
        raise @error if @error
      end

      def self.ok(sync, code, data)
        if code == 0
          new(sync, nil, data)
        else
          new(sync, ::Tarantool16::DBError.with_code_message(code, data), nil)
        end
      end

      def self.error(sync, err, message = nil)
        if err.is_a? Class
          err = err.new message
        end
        new(sync, err, nil)
      end

      def inspect
        s = @sync ? " sync=#{sync}" : ""
        if ok?
          "<Option#{s} data=#{@data.inspect}>"
        else
          "<Option#{s} error=#{@error.inspect}>"
        end
      end
    end
  end
end
