class Tarantool
  class Request

    class << self
      def request_type(name = nil)
        if name
          @request_type = Tarantool::Requests::REQUEST_TYPES[name] || raise(UndefinedRequestType)
        else
          @request_type
        end
      end

      def pack_tuple(*values)
        [values.size].pack('V') + values.map { |v| pack_field(v) }.join
      end

      def pack_field(value)
        case value
        when String
          raise StringTooLong.new if value.bytesize > 1024 * 1024
          [value.bytesize, value].pack('wa*')
        when Integer
          if value < 4294967296 # 2 ^ 32
            [4, value].pack('wV')
          else
            [8, value].pack('wQ<')
          end
        when Tarantool::Field
          [value.data.bytesize].pack('w') + value.data
        else
          raise ArgumentError.new("Field should be integer or string (got #{value.inspect})")
        end
      end 
    end

    attr_reader :space, :params, :args
    attr_reader :space_no
    def initialize(space, *args)
      @space = space      
      @args = args
      @params = args.last.is_a?(Hash) ? args.pop : {}
      @space_no = params.delete(:space_no) || space.space_no || raise(UndefinedSpace.new)
      parse_args
    end

    def perform
      data = connection.send_request self.class.request_type, make_body
      make_response data
    end

    def parse_args
      
    end

    def make_response(data)
      return_code, = data[0,4].unpack('V')
      if return_code == 0
        Response.new(data[4, data.size], response_params)
      else
        msg = data[4, data.size].unpack('A*')
        raise BadReturnCode.new("Error code #{return_code}: #{msg}")
      end
    end

    def response_params
      @params[:return_tuple] ? {return_tuple: true} : {}
    end

    def connection
      space.connection
    end
  end
end
