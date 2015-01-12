require 'tarantool16/errors'

module Tarantool16
  module Connection
    class Response
      def initialize(sync, retcode, data)
        @sync = sync
        if retcode == 0
          @error = nil
          @data = data
        elsif Integer === retcode
          @error = Tarantool16::DBError.with_code_message(retcode, data)
          @data = nil
        elsif ::StandardError === retcode
          @error = retcode
          @data = nil
        elsif retcode.is_a?(Class) && StandardError > retcode
          @error = retcode.new(data)
          @data = nil
        else
          raise "what are f**k?"
        end
      end

      attr :sync, :error, :data
      alias error? error

      def ok?
        @error.nil?
      end

      def inspect
        if ok?
          "<Tarantool16::Connection::Response data=#{@data.inspect}>"
        else
          "<Tarantool16::Connection::Response error=#{@error.inspect}>"
        end
      end
    end
  end
end
