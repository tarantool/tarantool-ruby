require 'em-tarantool/util'
require 'em-tarantool/request'
require 'em-tarantool/response'

module EM
  class Tarantool
    class Space
      include Request

      def initialize(tarantool, space_no, fields, indexes)
        @tarantool = tarantool
        @space_no = space_no
        @fields = fields
        indexes = indexes ? Array(indexes).map{|a| Array(a)} : [[0]]
        @indexes = indexes.map{|index| index.map{|i| @fields[i]}}
      end

      def _send_request(type, body, cb)
        @tarantool._send_request(type, body, cb)
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
        _select(@space_no, index_no, offset, limit, keys, cb, @fields, @indexes[index_no])
      end

      def insert(tuple, cb_or_opts = nil, opts = {}, &block)
        _insert(@space_no, BOX_ADD, tuple, @fields, cb_or_opts, opts, &block)
      end

      def replace(tuple, cb_or_opts = nil, opts = {}, &block)
        _insert(@space_no, BOX_REPLACE, tuple, @fields, cb_or_opts, opts, &block)
      end

      def update(pk, operations, cb_or_opts = nil, opts = {}, &block)
        _update(@space_no, pk, operations, @fields, @indexes[0], cb_or_opts, opts, &block)
      end

      def delete(pk, cb_or_opts = nil, opts = {}, &block)
        _delete(@space_no, pk, @fields, @indexes[0], cb_or_opts, opts, &block)
      end

    end
  end
end
