require 'tarantool/request'
require 'tarantool/response'

module Tarantool
  class Query
    include Request
    def initialize(tarantool)
      @tarantool = tarantool
      _init_shard_vars(nil, false)
    end

    def select_cb(space_no, index_no, keys, offset, limit, cb, opts={})
      keys = [*keys]
      types = opts[:types] || _detect_types(keys)
      returns = opts[:returns] || TYPES_AUTO
      if Hash === returns
        returns, *translators = _parse_hash_definition(returns)
      end
      shard_nums = _get_shard_nums{ all_shards }
      _select(space_no, index_no, offset, limit, keys, cb, returns,
              types, shard_nums, translators)
    end

    def all_cb(space_no, index_no, keys, cb, opts={})
      select_cb(space_no, index_no, keys,
                opts[:offset] || 0, opts[:limit] || -1,
                cb, opts)
    end

    def first_cb(space_no, index_no, key, cb, opts={})
      select_cb(space_no, index_no, [key], 0, :first, cb, opts)
    end

    def insert_cb(space_no, tuple, cb, opts={})
      types = opts[:types] || _detect_types(tuple)
      shard_nums = _get_shard_nums{ all_shards }
      _insert(space_no, BOX_ADD, tuple, types, cb, opts[:return_tuple], shard_nums)
    end

    def replace_cb(space_no, tuple, cb, opts={})
      types = opts[:types] || _detect_types(tuple)
      shard_nums = _get_shard_nums{ all_shards }
      _insert(space_no, BOX_REPLACE, tuple, types, cb, opts[:return_tuple], shard_nums)
    end

    def store_cb(space_no, tuple, cb, opts={})
      types = opts[:types] || _detect_types(tuple)
      shard_nums = _get_shard_nums{ all_shards }
      _insert(space_no, 0, tuple, types, cb, opts[:return_tuple], shard_nums)
    end

    def update_cb(space_no, pk, operations, cb, opts={})
      pk = [*pk]
      pk_types = opts[:pk_types] || _detect_types(pk)
      returns = opts[:returns] || TYPES_AUTO
      if Hash === returns && opts[:return_tuple]
        returns, *translators = _parse_hash_definition(returns)
      end
      shard_nums = _get_shard_nums{ all_shards }
      _update(space_no, pk, operations, returns, pk_types, cb,
              opts[:return_tuple], shard_nums, translators)
    end

    def delete_cb(space_no, pk, cb, opts={})
      pk = [*pk]
      pk_types = opts[:pk_types] || _detect_types(pk)
      returns = opts[:returns] || TYPES_AUTO
      if Hash === returns && opts[:return_tuple]
        returns, *translators = _parse_hash_definition(returns)
      end
      shard_nums = _get_shard_nums{ all_shards }
      _delete(space_no, pk, returns, pk_types, cb,
              opts[:return_tuple], shard_nums, translators)
    end

    def invoke_cb(func_name, values, cb, opts={})
      opts = opts.dup
      values = [*values]
      opts[:types] ||= _detect_types(values)
      _call(func_name, values, cb, opts)
    end

    def call_cb(func_name, values, cb, opts={})
      opts = opts.dup
      values = [*values]
      opts[:return_tuple] = true  if opts[:return_tuple].nil?
      opts[:types] ||= _detect_types(values)
      opts[:returns] ||= TYPES_AUTO
      if Hash === opts[:returns] && opts[:return_tuple]
        opts[:returns], *opts[:translators] = _parse_hash_definition(opts[:returns])
      end
      _call(func_name, values, cb, opts)
    end

    def select_blk(space_no, index_no, keys, offset, limit, opts={}, &block)
      select_cb(space_no, index_no, keys, offset, limit, block, opts)
    end

    def all_blk(space_no, index_no, keys, opts={}, &block)
      all_cb(space_no, index_no, keys, block, opts)
    end

    def first_blk(space_no, index_no, key, opts={}, &block)
      first_cb(space_no, index_no, key, block, opts)
    end

    def insert_blk(space_no, tuple, opts={}, &block)
      insert_cb(space_no, tuple, block, opts)
    end

    def replace_blk(space_no, tuple, opts={}, &block)
      replace_cb(space_no, tuple, block, opts)
    end

    def store_blk(space_no, tuple, opts={}, &block)
      store_cb(space_no, tuple, block, opts)
    end

    def update_blk(space_no, pk, operation, opts={}, &block)
      update_cb(space_no, pk, operation, block, opts={})
    end

    def delete_blk(space_no, pk, opts={}, &block)
      delete_cb(space_no, pk, block, opts)
    end

    def invoke_blk(func_name, values, opts={}, &block)
      invoke_cb(func_name, values, block, opts)
    end

    def call_blk(func_name, values, opts={}, &block)
      call_cb(func_name, values, block, opts)
    end

    def ping_blk(&block)
      ping_cb(block)
    end
  end
end
