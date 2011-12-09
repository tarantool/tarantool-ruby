module Tarantool
  class Request
    include EM::Deferrable

    class << self
      def request_type(name = nil)
        if name
          @request_type = Tarantool::Requests::REQUEST_TYPES[name] || raise(UndefinedRequestType)
        else
          @request_type
        end
      end

      def pack_tuple(*values)
        [values.size].pack('L') + values.map { |v| pack_field(v) }.join
      end

      def pack_field(value)
        if String === value
          raise StringTooLong.new if value.bytesize > 1024 * 1024
          [value.bytesize, value].pack('wa*')
        elsif Integer === value
          if value < 4294967296 # 2 ^ 32
            [4, value].pack('wL')
          else
            [8, value].pack('wQ')
          end
        elsif value.is_a?(Tarantool::Field)
          [value.data.bytesize].pack('w') + value.data
        else
          raise ArgumentError.new("Field should be integer or string")
        end
      end 
    end

    attr_reader :space, :params, :args
    attr_reader :space_no
    def initialize(space, *args)
      @space = space      
      @args = args
      @params = if args.last.is_a? Hash
        args.pop
      else
        {}
      end
      @space_no = params.delete(:space_no) || space.space_no || raise(UndefinedSpace.new)
      parse_args
    end

    def perform
      send_packet(make_packet(make_body))
      self
    end

    def parse_args
      
    end

    def request_id
      @request_id ||= connection.next_request_id
    end

    def make_packet(body)
      [self.class.request_type, body.size, request_id].pack('LLL') +
      body
    end

    def send_packet(packet)
      connection.send_packet request_id, packet do |data|
        make_response data
      end
    end

    def make_response(data)
      return_code,  = data[0,4].unpack('L')
      if return_code == 0
        succeed Response.new(data[4, data.size], response_params)
      else
        msg = data[4, data.size].unpack('A*')
        fail BadReturnCode.new("Error code #{return_code}: #{msg}")
      end
    end

    def response_params
      res = {}
      res[:return_tuple] = true if params[:return_tuple]
      res
    end

    def connection
      space.connection
    end
  end
end