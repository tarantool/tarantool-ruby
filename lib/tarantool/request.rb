require 'tarantool/util'

module Tarantool
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
    ZERO = "\x00".freeze
    EMPTY = "".freeze
    PACK_STRING = 'wa*'.freeze
    LEST_INT32 = -(2**31)
    GREATEST_INT32 = 2**32
    TYPES_STR = [:str].freeze
    TYPES_FALLBACK = [:str].freeze
    TYPES_STR_STR = [:str, :str].freeze

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
       '=' => 0, '+'  => 1, '&'  => 2, '^'  => 3, '|' => 4, '[]'    => 5,
                                                            ':'     => 5,
      :set => 0, :add => 1, :and => 2, :xor => 3, :or => 4, :splice => 5,
      'set'=> 0, 'add'=> 1, 'and'=> 2, 'xor'=> 3, 'or'=> 4, 'splice'=> 5,
      '#'     => 6, '!'     => 7,
      :delete => 6, :insert => 7,
      :del    => 6, :ins    => 7,
      'delete'=> 6, 'insert'=> 7,
      'del'   => 6, 'ins'   => 7
    }
    UPDATE_FIELDNO_OP = 'VC'.freeze

    def _select(space_no, index_no, offset, limit, keys, cb, fields, index_fields, translators = [])
      get_tuples = limit == :first ? (limit = 1; :first) : :all
      keys = Array(keys)
      body = [space_no, index_no, offset, limit, keys.size].pack(SELECT_HEADER)

      for key in keys
        pack_tuple(body, key, index_fields, index_no)
      end
      cb = ResponseWith.new(cb, get_tuples, fields, translators)
      _send_request(REQUEST_SELECT, body, cb)
    end

    class IndexIndexError < StandardError; end
    def pack_tuple(body, key, types, index_no = 0)
      if Integer === types.last
        *types, tail = types
      else
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
          field = types[i] || get_tail_item(types, i, tail)
          pack_field(body, field, key[i])
          i += 1
        end
      when nil
        body << INT32_0
      else
        body << INT32_1
        pack_field(body, types[0], key)
      end
    rescue IndexIndexError => e
      raise ValueError, "tuple #{key} has more entries than index #{index_no}"
    end

    def pack_field(body, field_kind, value)
      if value.nil?
        puts "NIL PACK"
        body << ZERO
        return
      end
      case field_kind
      when :int, :integer
        value = value.to_i
        if LEST_INT32 <= value && value < GREATEST_INT32
          body << BER4 << [value].pack(INT32)
        else
          body << BER8 << [value].pack(INT64)
        end
      when :error
        raise IndexIndexError
      when :str, :string
        value = value.to_s
        body << [value.bytesize, value].pack(PACK_STRING)
      else
        if serializer = field_kind.respond_to?(:encode) ? field_kind :
                        Tarantool::Serializers::MAP[field_kind]
          value = serializer.encode(value)
          body << [value.bytesize, value].pack(PACK_STRING)
        else
          raise ValueError, "Unknown field type #{field.inspect}"
        end
      end
    end

    def _modify_request(type, body, fields, ret_tuple, cb, translators)
      cb = ResponseWith.new(cb,
                           ret_tuple && (ret_tuple != :all ? :first : :all),
                           fields, translators)
      _send_request(type, body, cb)
    end

    def _insert(space_no, flags, tuple, fields, cb, ret_tuple, translators = [])
      flags |= BOX_RETURN_TUPLE  if ret_tuple
      fields = Array(fields)

      tuple = Array(tuple)
      tuple_size = tuple.size
      body = [space_no, flags].pack(INSERT_HEADER)
      pack_tuple(body, tuple, fields, :space)

      _modify_request(REQUEST_INSERT, body, fields, ret_tuple, cb, translators)
    end

    def _update(space_no, pk, operations, fields, pk_fields, cb, ret_tuple, translators = [])
      flags = ret_tuple ? BOX_RETURN_TUPLE : 0

      if Array === operations && !(Array === operations.first)
        operations = [operations]
      end

      body = [space_no, flags].pack(UPDATE_HEADER)
      pack_tuple(body, pk, pk_fields, 0)
      body << [operations.size].pack(INT32)

      _pack_operations(body, operations, fields)

      _modify_request(REQUEST_UPDATE, body, fields, ret_tuple, cb, translators)
    end

    def _pack_operations(body, operations, fields)
      if Integer === fields.last
        *fields, tail = fields
      else
        tail = 1
      end
      for operation in operations
        operation = operation.flatten
        field_no = operation[0]
        if operation.size == 2
          if (Integer === field_no || field_no =~ /\A\d/)
            unless Symbol === operation[1] && UPDATE_OPS[operation[1]] == 6
              body << [field_no, 0].pack(UPDATE_FIELDNO_OP)
              type = fields[field_no] || get_tail_item(fields, field_no, tail) ||
                _detect_type(operation[1])
              pack_field(body, type, operation[1])
              next
            end
          else
            operation.insert(1, field_no.slice(0, 1))
            field_no = field_no.slice(1..-1).to_i
          end
        end

        op = operation[1]
        op = UPDATE_OPS[op]  unless Integer === op
        raise ValueError, "Unknown operation #{operation[1]}" unless op
        body << [field_no, op].pack(UPDATE_FIELDNO_OP)
        case op
        when 0
          if (type = fields[field_no]).nil?
            if operation.size == 4 && Symbol === operation.last
              *operation, type = operation
            else
              type = get_tail_item(fields, field_no, tail) || _detect_type(operation[2])
            end
          end
          unless operation.size == 3
            raise ValueError, "wrong arguments for set or insert operation #{operation.inspect}"
          end
          pack_field(body, type, operation[2])
        when 1, 2, 3, 4
          unless operation.size == 3 && !operation[2].nil?
            raise ValueError, "wrong arguments for integer operation #{operation.inspect}"
          end
          pack_field(body, :int, operation[2])
        when 5
          unless operation.size == 5 && !operation[2].nil? && !operation[3].nil?
            raise ValueError, "wrong arguments for slice operation #{operation.inspect}"
          end

          str = operation[4].to_s
          body << [ 10 + ber_size(str.bytesize) + str.bytesize ].pack('w')
          pack_field(body, :int, operation[2])
          pack_field(body, :int, operation[3])
          pack_field(body, :str, str)
        when 7
          old_field_no = field_no + 
            (inserted ||= []).count{|i| i <= field_no} -
            (deleted ||= []).count{|i| i <= field_no}
          inserted << field_no
          if (type = fields[old_field_no]).nil?
            if operation.size == 4 && Symbol === operation.last
              *operation, type = operation
            else
              type = get_tail_item(fields, old_field_no, tail)
            end
          end
          unless operation.size == 3
            raise ValueError, "wrong arguments for set or insert operation #{operation.inspect}"
          end
          pack_field(body, type, operation[2])
        when 6
          body << ZERO
          # pass
        end
      end
    end

    def _delete(space_no, pk, fields, pk_fields, cb, ret_tuple, translators = [])
      flags = ret_tuple ? BOX_RETURN_TUPLE : 0

      body = [space_no, flags].pack(DELETE_HEADER)
      pack_tuple(body, pk, pk_fields, 0)

      _modify_request(REQUEST_DELETE, body, fields, ret_tuple, cb, translators)
    end

    def _space_call_fix_values(values, space_no, opts)
      opts = opts.dup
      space_no = opts[:space_no]  if opts.has_key?(:space_no)
      if space_no
        values = [space_no].concat(Array(values))
        if opts[:types]
          opts[:types] = [:str].concat(Array(opts[:types])) # cause lua could convert it to integer by itself
        else
          opts[:types] = TYPES_STR_STR
        end
      end
      [values, opts]
    end

    def _call(func_name, values, cb, opts={})
      return_tuple = opts[:return_tuple] && :all
      flags = return_tuple ? BOX_RETURN_TUPLE : 0
      
      values = Array(values)
      value_types = opts[:types] ? Array(opts[:types]) :
                                  _detect_types(values)
      return_types = Array(opts[:returns] || TYPES_STR)

      func_name = func_name.to_s
      body = [flags, func_name.size, func_name].pack(CALL_HEADER)
      pack_tuple(body, values, value_types, :func_call)

      _modify_request(REQUEST_CALL, body, return_types, return_tuple, cb, opts[:translators] || [])
    end

    def _ping(cb)
      _send_request(REQUEST_PING, EMPTY, cb)
    end
    alias ping_cb ping

    def _detect_types(values)
      values.map{|v| Integer === v ? :int : :str}
    end

    def _detect_type(value)
      Integer === v ? :int : :str
    end

    def _parse_hash_definition(returns)
      field_names = []
      field_types = []
      returns.each{|name, type|
        field_names << name
        field_types << type
      }
      field_types << if field_names.include?(:_tail)
          unless field_names.last == :_tail
            raise ValueError, "_tail should be de declared last"
          end
          Array(field_types.last).size
        else
          1
        end
      field_types.flatten!
      [field_types, TranslateToHash.new(field_names, field_types.last)]
    end

    def _raise_or_return(res)
      raise res  if Exception === res
      res
    end
  end

  module CommonSpaceBlockMethods
    def all_by_pks_blk(keys, opts={}, &block)
      all_by_pks_cb(keys, block, opts)
    end

    def by_pk_blk(key_array, &block)
      by_pk_cb(key_array, block)
    end

    def insert_blk(tuple, opts={}, &block)
      insert_cb(tuple, block, opts)
    end

    def replace_blk(tuple, opts={}, &block)
      replace_cb(tuple, block, opts)
    end

    def update_blk(pk, operations, opts={}, &block)
      update_cb(pk, operations, block, opts)
    end

    def delete_blk(pk, opts={}, &block)
      delete_cb(pk, block, opts)
    end

    def invoke_blk(func_name, values = [], opts={}, &block)
      invoke_cb(func_name, values, block, opts)
    end

    def call_blk(func_name, values = [], opts={}, &block)
      call_cb(func_name, values, block, opts)
    end

    def ping_blk(&block)
      ping_cb(block)
    end
  end

end
