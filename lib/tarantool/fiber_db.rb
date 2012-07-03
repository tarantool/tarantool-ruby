require 'tarantool/core-ext'
require 'tarantool/em_db'

module Tarantool
  class FiberDB < EMDB
    module CommonSpaceFiberMethods
      def all_by_pks(pks, opts={})
        all_by_pks_cb(pk, ::Fiber.current, opts)
        _fiber_result
      end

      def by_pk(pk)
        by_pk_cb(pk, ::Fiber.current)
        _fiber_result
      end

      def insert(tuple, opts={})
        insert_cb(tuple, ::Fiber.current, opts)
        _fiber_result
      end

      def replace(tuple, opts={})
        replace_cb(tuple, ::Fiber.current, opts)
        _fiber_result
      end

      def update(pk, operations, opts={})
        update_cb(pk, operations, ::Fiber.current, opts)
        _fiber_result
      end

      def delete(pk, opts={})
        delete_cb(pk, ::Fiber.current, opts)
        _fiber_result
      end

      def invoke(func_name, values = [], opts = {})
        invoke_cb(func_name, values, ::Fiber.current, opts)
        _fiber_result
      end

      def call(func_name, values = [], opts = {})
        call_cb(func_name, values, ::Fiber.current, opts)
        _fiber_result
      end
    end

    class SpaceArray < ::Tarantool::SpaceArray
      include CommonSpaceFiberMethods
      def all_by_key(index_no, key, opts={})
        all_by_key_cb(index_no, key, ::Fiber.current, opts)
        _fiber_result
      end

      def first_by_key(index_no, key)
        first_by_key_cb(index_no, key, ::Fiber.current)
        _fiber_result
      end

      def all_by_keys(index_no, key, opts={})
        all_by_keys_cb(index_no, key, ::Fiber.current, opts)
        _fiber_result
      end

      def select(index_no, offset, limit, keys)
        select_cb(index_no, offset, limit, keys, ::Fiber.current)
        _fiber_result
      end
    end

    class SpaceHash < ::Tarantool::SpaceHash
      include CommonSpaceFiberMethods

      def by_pk(key_array)
        by_pk_cb(key_array, ::Fiber.current)
        _fiber_result
      end

      def all(keys, opts = {})
        all_cb(keys, ::Fiber.current, opts)
        _fiber_result
      end

      def first(key)
        first_cb(key, ::Fiber.current)
        _fiber_result
      end

      def select(keys, offset, limit)
        select_cb(keys, offset, limit, ::Fiber.current)
        _fiber_result
      end
    end

    class Query < ::Tarantool::Query
      def select(space_no, index_no, keys, offset, limit, opts={})
        select_cb(space_no, index_no, keys, offset, limit, ::Fiber.current, opts)
        _fiber_result
      end

      def all(space_no, index_no, keys, opts={})
        all_cb(space_no, index_no, keys, ::Fiber.current, opts)
        _fiber_result
      end

      def first(space_no, index_no, key, opts={})
        first_cb(space_no, index_no, key, ::Fiber.current, opts)
        _fiber_result
      end

      def insert(space_no, tuple, opts={})
        insert_cb(space_no, tuple, ::Fiber.current, opts)
        _fiber_result
      end

      def replace(space_no, tuple, opts={})
        replace_cb(space_no, tuple, ::Fiber.current, opts)
        _fiber_result
      end

      def update(space_no, pk, operation, opts={})
        update_cb(space_no, pk, operation, ::Fiber.current, opts)
        _fiber_result
      end

      def delete(space_no, pk, opts={})
        delete_cb(space_no, pk, ::Fiber.current, opts)
        _fiber_result
      end

      def invoke(func_name, values, opts={})
        invoke_cb(func_name, values, ::Fiber.current, opts)
        _fiber_result
      end

      def call(func_name, values, opts={})
        call_cb(func_name, values, ::Fiber.current, opts)
        _fiber_result
      end
    end
  end
end
