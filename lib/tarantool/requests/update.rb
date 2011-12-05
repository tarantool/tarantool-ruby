module Tarantool
  module Requests
    class Update < Request
      request_type :update

      OP_CODES = { set: 0, add: 1, and: 2, or: 3, xor: 4, splice: 5 }

      def self.pack_ops(ops)
        ops.map do |op|
          raise ArgumentError.new('Operation should be array of size 3') unless op.size == 3

          field_no, op_symbol, op_arg = op
          op_code = OP_CODES[op_symbol] || raise(ArgumentError.new("Unsupported operation symbol '#{op_symbol}'"))

          [field_no, op_code].pack('LC') + self.pack_field(op_arg)
        end.join
      end

      attr_reader :flags, :key, :ops
      def parse_args
          @flags = params[:return_tuple] ? 1 : 0
          @key = params[:key] || args.first
          @ops = params[:ops]
          raise ArgumentError.new('Key is required') unless key
      end

      def make_body
        [space_no, flags].pack('LL') +
        self.class.pack_tuple(key) +
        [ops.size].pack('L') +
        self.class.pack_ops(ops)
      end
    end
  end
end