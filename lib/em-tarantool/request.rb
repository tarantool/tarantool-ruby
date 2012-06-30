require 'em-tarantool/util'
module EM
  class Tarantool
    module Request
      include Util::Packer
      include Util::TailGetter
      INT32 = 'V'.freeze
      INT64 = 'Q<'.freeze
      SELECT_HEADER = 'VVVVV'.freeze
      INSERT_HEADER = 'VV'.freeze
      UPDATE_HEADER = 'VV'.freeze
      DELETE_HEADER = 'VV'.freeze
      CALL_HEADER = 'Vwa*'.freeze
      INT32_0 = "\x00\x00\x00\x00".freeze
      INT32_1 = "\x01\x00\x00\x00".freeze
      BER4 = "\x04".freeze
      BER8 = "\x08".freeze
      PACK_STRING = 'wa*'.freeze
      LEST_INT32 = -(2**31)
      GREATEST_INT32 = 2**32
      TYPES_STR = [:str].freeze
      TYPES_FALLBACK = [:str].freeze
      TYPES_INT_STR = [:int, :str].freeze

      REQUEST_SELECT = 17
      REQUEST_INSERT = 13
      REQUEST_UPDATE = 19
      REQUEST_DELETE = 21
      REQUEST_CALL   = 22
      REQUEST_PING   = 65280

      BOX_RETURN_TUPLE = 0x01
      BOX_ADD = 0x02
      BOX_REPLACE = 0x04

      UPDATE_OPS = {
        :"=" => 0, :+   => 1, :&   => 2, :^   => 3, :|  => 4, :[]     => 5,
        :set => 0, :add => 1, :and => 2, :xor => 3, :or => 4, :splice => 5, :delete => 6, :insert => 7,
                                                                            :del    => 6, :ins    => 7,
        'set'=> 0, 'add'=> 1, 'and'=> 2, 'xor'=> 3, 'or'=> 4, 'splice'=> 5, 'delete'=> 6, 'insert'=> 7,
                                                                            'del'   => 6, 'ins'   => 7
      }
      UPDATE_FIELDNO_OP = 'VC'.freeze

      def _select(space_no, index_no, offset, limit, keys, cb, fields=nil, index_fields=nil)
        keys = Array(keys)
        body = [space_no, index_no, offset, limit, keys.size].pack(SELECT_HEADER)

        for key in keys
          pack_key_tuple(body, key, index_fields, :error, index_no)
        end
        cb = ResponseWithTuples.new(cb || block, fields, false)
        _send_request(REQUEST_SELECT, body, cb)
        cb
      end

      def pack_key_tuple(body, key, types, tail = :error, index_no = 0)
        if types.equal?(TYPES_FALLBACK) || tail == :last
          tail = 1
        end
        case key
        when Array
          if nili = key.index(nil)
            key = key.slice(0, nili)
          end
          body << [key_size = key.size].pack(INT32)
          i = 0
          while i < key_size
            if (field = types[i]).nil?
              case tail
              when :error
                raise ValueError, "tuple #{key} has more entries than index #{index_no}"
              when Integer
                field = get_tail_item(types, i, tail)
              end
            end
            pack_key(body, field, key[i])
            i += 1
          end
        when nil
          body << INT32_0
        else
          body << INT32_1
          pack_key(body, types[0], key)
        end
      end

      def pack_key(body, field_kind, value)
        case field_kind
        when :int
          value = value.to_i
          if LEST_INT32 <= value && value < GREATEST_INT32
            body << BER4 << [value].pack(INT32)
          else
            body << BER8 << [value].pack(INT64)
          end
        else
          value = value.to_s
          body << [value.bytesize, value].pack(PACK_STRING)
        end
      end

      def _modify_request(type, body, fields, opts, cb)
        cb = case opts[:return_tuple]
             when nil, false
               ResponseWithoutTuples.new(cb)
             when :all
               ResponseWithTuples.new(cb, fields, false)
             else
               ResponseWithTuples.new(cb, fields, true)
             end
        _send_request(type, body, cb)
      end

      def _insert(space_no, flags, tuple, fields, cb_or_opts = nil, opts = {}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        flags |= (opts[:return_tuple] ? BOX_RETURN_TUPLE : 0)
        fields = Array(fields)
        *fields, tail = fields  if Integer === fields.last

        tuple = Array(tuple)
        tuple_size = tuple.size
        body = [space_no, flags].pack(INSERT_HEADER)
        pack_key_tuple(body, tuple, fields, tail || 1, :space)

        _modify_request(REQUEST_INSERT, body, fields, opts, cb_or_opts || block)
      end

      def _update(space_no, pk, operations, fields, pk_fields, cb_or_opts = nil, opts = {}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        flags = opts[:return_tuple] ? BOX_RETURN_TUPLE : 0
        *fields, tail = fields  if Integer === fields.last

        if Array === operations && !(Array === operations.first)
          operations = [operations]
        end

        body = [space_no, flags].pack(UPDATE_HEADER)
        pack_key_tuple(body, pk, pk_fields, :error)
        body << [operations.size].pack(INT32)

        _pack_operations(body, operations, fields, tail || 1)

        _modify_request(REQUEST_UPDATE, body, fields, opts, cb_or_opts || block)
      end

      def _pack_operations(body, operations, fields, tail)
        for operation in operations
          operation.flatten!
          field_no = operation[0]
          if operation.size == 2 && UPDATE_OPS[operation[1]] != 6
            body << [field_no, 0].pack(UPDATE_FIELDNO_OP)
            pack_key(body, fields[field_no], operation[1])
          else
            op = operation[1]
            op = UPDATE_OPS[op]  unless Integer === op
            raise ValueError, "Unknown operation #{operation[1]}" unless op
            body << [field_no, op].pack(UPDATE_FIELDNO_OP)
            case op
            when 0, 7
              if (type = fields[field_no]).nil?
                if operation.size == 4 && Symbol === operation.last
                  *operation, type = operation
                else
                  type = get_tail_item(fields, field_no, tail)
                end
              end
              unless operation.size == 3
                raise ValueError, "wrong arguments for set or insert operation #{operation.inspect}"
              end
              pack_key(body, fields[field_no], operation[2])
            when 1, 2, 3, 4
              unless operation.size == 3 && !operation[2].nil?
                raise ValueError, "wrong arguments for integer operation #{operation.inspect}"
              end
              pack_key(body, :int, operation[2])
            when 5
              unless operation.size == 5 && !operation[2].nil? && !operation[3].nil?
                raise ValueError, "wrong arguments for slice operation #{operation.inspect}"
              end

              str = operation[4].to_s
              body << [ 10 + ber_size(str.bytesize) + str.bytesize ].pack('w')
              pack_key(body, :int, operation[2])
              pack_key(body, :int, operation[3])
              pack_key(body, :str, str)
            when 6
              body << "\x00"
              # pass
            end
          end
        end
      end

      def _delete(space_no, pk, fields, pk_fields, cb_or_opts = nil, opts = {}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        flags = opts[:return_tuple] ? BOX_RETURN_TUPLE : 0

        body = [space_no, flags].pack(DELETE_HEADER)
        pack_key_tuple(body, pk, pk_fields, :error)

        _modify_request(REQUEST_DELETE, body, fields, opts, cb_or_opts || block)
      end

      def _call(func_name, values, cb_or_opts = nil, opts={}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        flags = opts[:return_tuple] ? BOX_RETURN_TUPLE : 0
        opts[:return_tuple] = :all  if opts[:return_tuple]

        value_types = Array(opts[:types] || _detect_types(values))
        return_types = Array(opts[:returns] || TYPES_STR)

        func_name = func_name.to_s

        body = [flags, func_name.size, func_name].pack(CALL_HEADER)
        pack_key_tuple(body, values, value_types, opts[:tail] || 1)

        _modify_request(REQUEST_CALL, body, return_types, opts, cb_or_opts || block)
      end

      def _detect_types(values)
        values.map{|v| Integer === v ? :int : :str}
      end

    end
  end
end
