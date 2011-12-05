module Tarantool
  module Requests
    class Insert < Request
      request_type :insert

      attr_reader :flags, :values
      def parse_args
        @flags = BOX_ADD 
        @flags |= BOX_RETURN_TUPLE if params[:return_tuple]
        @values = params[:values] || args
      end

      def make_body
        [space_no, flags].pack('LL') +
        self.class.pack_tuple(*values)
      end
    end
  end
end