require 'em-tarantool/util'
require 'em-tarantool/response'

module EM
  class Tarantool
    class Space
      INT32 = 'V'.freeze
      INT64 = 'Q<'.freeze
      SELECT_HEADER = 'VVVVV'.freeze
      INSERT_HEADER = 'VVV'.freeze
      UPDATE_HEADER = 'VV'.freeze
      DELETE_HEADER = 'VV'.freeze
      INT32_0 = "\x00\x00\x00\x00".freeze
      INT32_1 = "\x01\x00\x00\x00".freeze
      BER4 = "\x04".freeze
      BER8 = "\x08".freeze
      PACK_STRING = 'wa*'.freeze
      LEST_INT32 = -(2**31)
      GREATEST_INT32 = 2**32

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

      def initialize(tarantool, space_no, fields, indexes)
        @tarantool = tarantool
        @space_no = space_no
        @fields = fields
        indexes = indexes ? Array(indexes).map{|a| Array(a)} : [[0]]
        @indexes = indexes.map{|index| index.map{|i| @fields[i]}}
      end

      def by_pk(pk, cb=nil, &block)
        first_by_key(0, pk, cb=nil, &block)
      end

      def all_by_key(index_no, key, cb_or_opts=nil, opts={}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        cb = cb_or_opts || block
        select(index_no, opts[:offset] || 0, opts[:limit] || -1, [key], cb)
      end

      class FirstCB < Struct.new(:cb)
        def call(tuples)
          if Exception === tuples
            cb.call tuples
          else
            cb.call(tuples.first)
          end
        end
      end

      def first_by_key(index_no, key, cb=nil, &block)
        select(index_no, 0, 1, [key], FirstCB.new(cb || block))
      end

      def all_by_keys(index_no, keys, cb_or_opts = nil, opts = {}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        cb = cb_or_opts || block
        select(index_no, opts[:offset] || 0, opts[:limit] || -1, keys, cb)
      end

      def select(index_no, offset, limit, keys, cb=nil, &block)
        body = [@space_no, index_no, offset, limit, keys.size].pack(SELECT_HEADER)
        index_fields = @indexes[index_no]

        for key in keys
          pack_key_tuple(body, key, index_fields)
        end
        cb = ResponseWithTuples.new(cb || block, @fields)
        @tarantool._send_request(REQUEST_SELECT, body, cb)
      end

      def pack_key_tuple(body, key, index_fields)
        case key
        when Array
          key = key.take_while{|v| !v.nil?}
          body << [key_size = key.size].pack(INT32)
          i = 0
          while i < key_size
            unless field = index_fields[i]
              raise ValueError, "Key #{key} has more entries than index #{index_no}"
            end
            pack_key(body, field, key[i])
            i += 1
          end
        when nil
          body << INT32_0
        else
          body << INT32_1
          pack_key(body, index_fields[0], key)
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

      def _modify_request(type, body, opts, cb)
        cb = opts[:return_tuple] ?
           ResponseWithTuples.new(cb, @fields) :
           ResponseWithoutTuples.new(cb)
        @tarantool._send_request(type, body, cb)
      end

      def _insert(flags, tuple, cb_or_opts = nil, opts = {}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        flags |= (opts[:return_tuple] ? BOX_RETURN_TUPLE : 0)

        tuple = Array(tuple)
        tuple_size = tuple.size
        body = [@space_no, flags, tuple_size].pack(INSERT_HEADER)
        i = 0
        fields = @fields
        while i < tuple_size
          pack_key(body, fields[i], tuple[i])
          i += 1
        end

        _modify_request(REQUEST_INSERT, body, opts, cb_or_opts || block)
      end

      def insert(tuple, cb_or_opts = nil, opts = {}, &block)
        _insert(BOX_ADD, tuple, cb_or_opts, opts, &block)
      end

      def replace(tuple, cb_or_opts = nil, opts = {}, &block)
        _insert(BOX_REPLACE, tuple, cb_or_opts, opts, &block)
      end

      def update(pk, operations, cb_or_opts = nil, opts = {}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        flags = opts[:return_tuple] ? BOX_RETURN_TUPLE : 0

        body = [@space_no, flags].pack(UPDATE_HEADER)
        pack_key_tuple(body, pk, @indexes[0])
        body << [operations.size].pack(INT32)

        fields = @fields

        for operation in operations
          operation.flatten!
          field_no = operation[0]
          if operation.size == 2
            body << [field_no, 0].pack(UPDATE_FIELDNO_OP)
            pack_key(body, fields[field_no], operation[1])
          else
            op = operation[1]
            op = UPDATE_OPS[op]  unless Integer === op
            body << [field_no, op].pack(UPDATE_FIELDNO_OP)
            case op
            when 0, 7
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
              unless operation.size == 5 && operation.all{|v| !v.nil?}
                raise ValueError, "wrong arguments for slice operation #{operation.inspect}"
              end
              pack_key(body, :int, operation[2])
              pack_key(body, :int, operation[3])
              pack_key(body, :str, operation[4])
            when 6
              # pass
            end
          end
        end

        _modify_request(REQUEST_UPDATE, body, opts, cb_or_opts || block)
      end

      def delete(pk, cb_or_opts = nil, opts = {}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        flags = opts[:return_tuple] ? BOX_RETURN_TUPLE : 0

        body = [@space_no, flags].pack(DELETE_HEADER)
        pack_key_tuple(body, pk, @indexes[0])

        _modify_request(REQUEST_DELETE, body, opts, cb_or_opts || block)
      end

    end
  end
end
