require 'tarantool/util'
require 'tarantool/request'
require 'tarantool/response'
require 'tarantool/core-ext'

module Tarantool
  class SpaceArray
    include Request

    def initialize(tarantool, space_no, fields, primary_index, indexes, shard_fields = nil, shard_proc = nil)
      @tarantool = tarantool
      @space_no = space_no
      @fields = (fields.empty? ? TYPES_STR : fields).dup.freeze
      indexes = Array(indexes).map{|ind| Array(ind)}
      @shard_fields = shard_fields || primary_index
      unless @shard_fields || @tarantool.shards_count == 1
        raise ArgumentError, "You could not use space without specifying primary key or shard fields when shards count is greater than 1"
      end
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
      @shard_positions = @shard_fields
      _init_shard_vars(shard_proc)
    end

    def _map_indexes(indexes)
      indexes.map do |index|
        index.map do |i|
          unless Symbol === (field = @fields[i])
            raise ArgumentError, "Wrong index field number: #{index} #{i}"
          end
          field
        end << :error
      end
    end

    def all_by_pks_cb(keys, cb, opts={})
      all_by_keys_cb(0, keys, cb, opts)
    end

    def by_pk_cb(pk, cb)
      first_by_key_cb(0, pk, cb)
    end

    def all_by_key_cb(index_no, key, cb, opts={})
      select_cb(index_no, opts[:offset] || 0, opts[:limit] || -1, [key], cb)
    end

    def first_by_key_cb(index_no, key, cb)
      select_cb(index_no, 0, :first, [key], cb)
    end

    def all_by_keys_cb(index_no, keys, cb, opts = {})
      select_cb(index_no, opts[:offset] || 0, opts[:limit] || -1, keys, cb)
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

    def select_cb(index_no, offset, limit, keys, cb)
      if Array === index_no
        raise ArgumentError, "Has no defined indexes to search index #{index_no}"  unless @index_fields
        index_fields = index_no
        index_no = @index_fields.index{|fields| fields.take(index_fields.size) == index_fields}
        unless index_no || index_fields.size == 1
          index_no, keys = _fix_index_fields(index_fields, keys)
          unless index_no
             raise(ArgumentError, "Not found index with field numbers #{index_no}, " +
                               "(defined indexes #{@index_fields})")
          end
        end
      end
      unless index_types = (@index_fields ? @indexes[index_no] : TYPES_FALLBACK)
        raise ArgumentError, "No index ##{index_no}"
      end

      shard_nums = _get_shard_nums { _detect_shards_for_keys(keys, index_no) }

      _select(@space_no, index_no, offset, limit, keys, cb, @fields, shard_nums, index_types)
    end

    def insert_cb(tuple, cb, opts = {})
      shard_nums = _get_shard_nums{ detect_shard(tuple.values_at(*@shard_positions)) }
      _insert(@space_no, BOX_ADD, tuple, @fields, cb, opts[:return_tuple], shard_nums)
    end

    def replace_cb(tuple, cb, opts = {})
      shard_nums = _get_shard_nums{ detect_shard(tuple.values_at(*@shard_positions)) }
      _insert(@space_no, BOX_REPLACE, tuple, @fields,
              cb, opts[:return_tuple], shard_nums)
    end

    def update_cb(pk, operations, cb, opts = {})
      shard_nums = _get_shard_nums{ _detect_shard_for_key(pk, 0) }
      _update(@space_no, pk, operations, @fields,
              @indexes[0], cb, opts[:return_tuple],
              shard_nums)
    end

    def delete_cb(pk, cb, opts = {})
      shard_nums = _get_shard_nums{ _detect_shard_for_key(pk, 0) }
      _delete(@space_no, pk, @fields, @indexes[0], cb, opts[:return_tuple], shard_nums)
    end

    def invoke_cb(func_name, values, cb, opts = {})
      values, opts = _space_call_fix_values(values, @space_no, opts)
      _call(func_name, values, cb, opts)
    end

    def call_cb(func_name, values, cb, opts = {})
      values, opts = _space_call_fix_values(values, @space_no, opts)

      opts[:return_tuple] = true  if opts[:return_tuple].nil?
      opts[:returns] ||= @fields   if opts[:return_tuple]

      _call(func_name, values, cb, opts)
    end

    include CommonSpaceBlockMethods
    # callback with block api
    def all_by_key_blk(index_no, key, opts={}, &block)
      all_by_key_cb(index_no, key, block, opts)
    end

    def first_by_key_blk(index_no, key, &block)
      first_by_key_cb(index_no, key, block)
    end

    def all_by_keys_blk(index_no, keys, opts={}, &block)
      all_by_keys_cb(index_no, keys, block, opts)
    end

    def select_blk(index_no, offset, limit, keys, &block)
      select_cb(index_no, offset, limit, keys, block)
    end
  end
end
