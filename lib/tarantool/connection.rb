require 'em/protocols/fixed_header_and_body'
module Tarantool
  class Connection < EM::Connection
    include EM::Protocols::FixedHeaderAndBody

    header_size 12

    def next_request_id
      @next_request_id ||= 0
      @next_request_id += 1
      if @next_request_id > 0xffffffff
        @next_request_id = 0
      end
      @next_request_id
    end

    def connection_completed
      @connected = true
    end

    # begin FixedHeaderAndBody API
    def body_size
      @body_size
    end

    def receive_header(header)
      @type, @body_size, @request_id = header.unpack('L3')
    end

    def receive_body(data)
      clb = waiting_requests.delete @request_id
      raise UnexpectedResponse.new("For request id #{@request_id}") unless clb
      clb.call data
    end
    # end FixedHeaderAndBody API

    def waiting_requests
      @waiting_requests ||= {}
    end

    def send_packet(request_id, data, &clb)
      send_data data
      waiting_requests[request_id] = clb
    end

    def close_connection(*args)
      super(*args)
    end

    def unbind
      raise CouldNotConnect.new unless @connected
    end
  end
end