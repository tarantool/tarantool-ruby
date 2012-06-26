class Tarantool
  module Requests
    class Select < Request
      request_type :select

      attr_reader :index_no, :offset, :limit, :count, :tuples
      def parse_args
        @index_no = params[:index_no] || 0
        @offset = params[:offset] || 0
        @limit = params[:limit] || -1
        @tuples = params[:values] || args
        raise(ArgumentError.new('values are required')) if tuples.empty?
        params[:return_tuple] = true
      end

      def make_body
        [space_no, index_no, offset, limit, tuples.size].pack('VVVVV') +
        tuples.map { |tuple| self.class.pack_tuple(*tuple) }.join
      end
    end
  end
end
