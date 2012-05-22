module Tarantool
  module Requests
    class Delete < Request
      request_type :delete

      attr_reader :flags, :key
      def parse_args
        @flags = params[:return_tuple] ? 1 : 0
        @key = params[:key] || args.first
      end

      def make_body
        [space_no, flags].pack('LL') +
        self.class.pack_tuple(*key)
      end
    end
  end
end