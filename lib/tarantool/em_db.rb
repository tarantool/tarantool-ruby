module Tarantool
  class EMDB < DB
    def establish_connection
      @connection_waiters = []
      EM.schedule do
        unless @closed
          @connection = IProto.get_connection(@host, @port, :em_callback)
          while waiter = @connection_waiters.shift
            _send_request(*waiter)
          end
        end
      end
    end

    def close_connection
      EM.schedule do
        if @connection
          @connection.close
          @connection = nil
        end
        unless @connection_waiters.empty?
          while waiter = @connection_waiters.shift
            waiter.last.call(::IProto::Disconnected)
          end
        end
      end
    end

    def _send_request(request_type, body, cb)
      if @connection
        @connection.send_request(request_type, body, cb)
      else
        @connection_waiters << [request_type, body, cb]
      end
    end
  end
end
