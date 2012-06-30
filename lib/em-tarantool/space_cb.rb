require 'em-tarantool/util'
require 'em-tarantool/request'
require 'em-tarantool/response'

module EM
  class Tarantool
    class SpaceCB
      include Request

      def initialize(tarantool, space_no, fields, primary_index, indexes)
        @tarantool = tarantool
        @space_no = space_no
        @fields = (fields.empty? ? TYPES_STR : fields).dup.freeze
        indexes = Array(indexes)
        if primary_index
          indexes = [Array(primary_index)].concat(indexes)
          @index_fields = indexes
          @indexes = _map_indexes(indexes)
        elsif !indexes.empty?
          @index_fields = [[]] + indexes
          @indexes = [TYPES_FALLBACK] + _map_indexes(indexes)
        else
          @index_fields = nil
          @indexes = [TYPES_FALLBACK]
        end
      end

      def _map_indexes(indexes)
        indexes.map do |index|
          index.map do |i|
            unless Symbol === (field = @fields[i])
              raise "Wrong index field number: #{index} #{i}"
            end
            field
          end << :error
        end
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

      def first_by_key(index_no, key, cb=nil, &block)
        select(index_no, 0, 1, [key], cb, &block).first = true
      end

      def all_by_keys(index_no, keys, cb_or_opts = nil, opts = {}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        cb = cb_or_opts || block
        select(index_no, opts[:offset] || 0, opts[:limit] || -1, keys, cb)
      end

      def _fix_index_fields(index_fields, keys)
        sorted = index_fields.sort
        if index_no = @index_fields.index{|fields| fields.take(index_fields.size).sort == sorted}
          real_fields = @index_fields[index_no]
          permutation = index_fields.map{|i| real_fields.index(i)}
          keys = Array(keys).map{|v| Array(v).values_at(*permutation)}
          [index_no, keys]
        end
      end

      def select(index_no, offset, limit, keys, cb=nil, &block)
        if Array === index_no
          raise ValueError, "Has no defined indexes to search index #{index_no}"  unless @index_fields
          index_fields = index_no
          index_no = @index_fields.index{|fields| fields.take(index_fields.size) == index_fields}
          unless index_no || index_fields.size == 1
            index_no, keys = _fix_index_fields(index_fields, keys)
            unless index_no
               raise(ValueError, "Not found index with field numbers #{index_no}, " +
                                 "(defined indexes #{@index_fields})")
            end
          end
        end
        unless index_types = (@index_fields ? @indexes[index_no] : TYPES_FALLBACK)
          raise ValueError, "No index ##{index_no}"
        end

        _select(@space_no, index_no, offset, limit, keys, cb || block,
                @fields, index_types)
      end

      def insert(tuple, cb_or_opts = nil, opts = {}, &block)
        _insert(@space_no, BOX_ADD, tuple, @fields, cb_or_opts, opts, &block)
      end

      def replace(tuple, cb_or_opts = nil, opts = {}, &block)
        _insert(@space_no, BOX_REPLACE, tuple, @fields, cb_or_opts, opts, &block)
      end

      def update(pk, operations, cb_or_opts = nil, opts = {}, &block)
        _update(@space_no, pk, operations, @fields,
                @indexes[0], cb_or_opts, opts, &block)
      end

      def delete(pk, cb_or_opts = nil, opts = {}, &block)
        _delete(@space_no, pk, @fields,
                @indexes[0], cb_or_opts, opts, &block)
      end

      def invoke(func_name, values, cb_or_opts = nil, opts = {}, &block)
        values.unshift(@space_no)
        if opts[:types]
          opts[:types].unshift(:int)
        else
          opts[:types] = TYPES_INT_STR
        end
        _call(func_name, values, cb_or_opts, opts, &block)
      end

      def call(func_name, values, cb_or_opts = nil, opts = {}, &block)
        if Hash === cb_or_opts
          opts = cb_or_opts
          cb_or_opts = nil
        end
        opts[:return_tuples] = true  if opts[:return_tuple].nil?
        opts[:returns] ||= @fields   if opts[:return_tuple]

        values.unshift(@space_no)
        if opts[:types]
          opts[:types].unshift(:int)
        else
          opts[:types] = TYPES_INT_STR
        end
        _call(func_name, values, cb_or_opts, opts, &block)
      end

    end
  end
end
