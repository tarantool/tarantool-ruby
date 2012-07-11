module Tarantool
  class BlockDB < DB
    IPROTO_CONNECTION_TYPE = :block

    def _send_to_one_shard(shard_number, read_write, request_type, body, cb)
      cb.call(
        if (replicas = _shard(shard_number)).size == 1
          replicas[0].send_request(request_type, body)
        elsif read_write == :read
          replicas = replicas.shuffle  if @shard_strategy == :round_robin
          _one_shard_read(replicas, request_type, body)
        else
          _one_shard_write(replicas, request_type, body)
        end
      )
    end

    def _one_shard_read(replicas, request_type, body)
      for conn in replicas
        if conn.could_be_connected?
          begin
            res = conn.send_request(request_type, body)
          rescue ::IProto::ConnectionError
            # pass
          else
            return res
          end
        end
      end
      raise ConnectionError, "no available connections"
    end

    def _one_shard_write(replicas, request_type, body)
      i = replicas.size
      while i > 0
        conn = replicas[0]
        if conn.could_be_connected?
          begin
            res = conn.send_request(request_type, body)
          rescue ::IProto::ConnectionError, ::Tarantool::NonMaster
            # pass
          else
            return res
          end
        end
        replicas.rotate!
        i -= 1
      end
      raise NoMasterError, "no available master connections"
    end

    def _raise_or_return(res)
      raise res  if Exception == res
      res
    end

    def _send_to_several_shards(shard_numbers, read_write, request_type, body, cb)
      @_several_shards_cb ||= method(:_raise_or_return)
      results = []
      for shard in shard_numbers
        res = _send_to_one_shard(shard, read_write, request_type, body, @_several_shards_cb)
        if Array === res
          results.concat res
        else
          results << res
        end
      end
      if Integer === results.first
        results = results.inject(0){|s, i| s + i}
      end
      cb.call results
    end

    module CommonSpaceBlockingMethods
      def _block_cb
        @_block_cb ||= method(:_raise_or_return)
      end

      def all_by_pks(pks, opts={})
        all_by_pks_cb(pks, _block_cb, opts)
      end

      def by_pk(pk)
        by_pk_cb(pk, _block_cb)
      end

      def insert(tuple, opts={})
        insert_cb(tuple, _block_cb, opts)
      end

      def replace(tuple, opts={})
        replace_cb(tuple, _block_cb, opts)
      end

      def update(pk, operations, opts={})
        update_cb(pk, operations, _block_cb, opts)
      end

      def delete(pk, opts={})
        delete_cb(pk, _block_cb, opts)
      end

      def invoke(func_name, values = [], opts = {})
        invoke_cb(func_name, values, _block_cb, opts)
      end

      def call(func_name, values = [], opts = {})
        call_cb(func_name, values, _block_cb, opts)
      end

      def ping
        ping_cb(_block_cb)
      end
    end

    class SpaceArray < ::Tarantool::SpaceArray
      include CommonSpaceBlockingMethods

      def all_by_key(index_no, key, opts={})
        all_by_key_cb(index_no, key, _block_cb, opts)
      end

      def first_by_key(index_no, key)
        first_by_key_cb(index_no, key, _block_cb)
      end

      def all_by_keys(index_no, key, opts={})
        all_by_keys_cb(index_no, key, _block_cb, opts)
      end

      def select(index_no, offset, limit, keys)
        select_cb(index_no, offset, limit, keys, _block_cb)
      end
    end

    class SpaceHash < ::Tarantool::SpaceHash
      include CommonSpaceBlockingMethods

      def all(keys, opts = {})
        all_cb(keys, _block_cb, opts)
      end

      def first(key)
        first_cb(key, _block_cb)
      end

      def select(keys, offset, limit)
        select_cb(keys, offset, limit, _block_cb)
      end
    end

    class Query < ::Tarantool::Query
      def select(space_no, index_no, keys, offset, limit, opts={})
        select_cb(space_no, index_no, keys, offset, limit, _block_cb, opts)
      end

      def all(space_no, index_no, keys, opts={})
        all_cb(space_no, index_no, keys, _block_cb, opts)
      end

      def first(space_no, index_no, key, opts={})
        first_cb(space_no, index_no, key, _block_cb, opts)
      end

      def insert(space_no, tuple, opts={})
        insert_cb(space_no, tuple, _block_cb, opts)
      end

      def replace(space_no, tuple, opts={})
        replace_cb(space_no, tuple, _block_cb, opts)
      end

      def update(space_no, pk, operation, opts={})
        update_cb(space_no, pk, operation, _block_cb, opts)
      end

      def delete(space_no, pk, opts={})
        delete_cb(space_no, pk, _block_cb, opts)
      end

      def invoke(func_name, values, opts={})
        invoke_cb(func_name, values, _block_cb, opts)
      end

      def call(func_name, values, opts={})
        call_cb(func_name, values, _block_cb, opts)
      end

      def ping
        ping_cb(_block_cb)
      end
    end
  end
end
