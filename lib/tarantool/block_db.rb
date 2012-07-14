module Tarantool
  class BlockDB < DB
    def establish_connection
      @connection = IProto.get_connection(@host, @port, :block)
    end

    def close_connection
      @connection.close
    end

    def _send_request(request_type, body, cb)
      cb.call @connection.send_request(request_type, body)
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

      def all_by_keys(index_no, keys, opts={})
        all_by_keys_cb(index_no, keys, _block_cb, opts)
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
