class Tarantool
  module Requests
    class Ping < Request
      request_type :ping

      def make_body
        @start_time = Time.now
        ''
      end

      def make_response(data)
        Time.now - @start_time
      end
    end
  end
end