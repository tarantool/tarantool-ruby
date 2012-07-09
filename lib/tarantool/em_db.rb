module Tarantool
  class EMDB < DB
    IPROTO_CONNECTION_TYPE = :em_callback

    def _send_request(server_names, strategy, request_type, body, cb)
      if @closed
        cb.call(::IProto::Disconnected.new("Tarantool is closed"))
      else
        _connection(server_name).send_request(request_type, body, cb)
      end
    end
  end
end
