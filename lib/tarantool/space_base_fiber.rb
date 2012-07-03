require "tarantool/space_base"

class Tarantool
  class SpaceBaseFiber < SpaceBase
    alias by_pk        by_pk_fib
    alias all_by_key   all_by_key_fib
    alias first_by_key first_by_key_fib
    alias all_by_keys  all_by_keys_fib
    alias select       select_fib
    alias insert       insert_fib
    alias replace      replace_fib
    alias update       update_fib
    alias delete       delete_fib
    alias invoke       invoke_fib
    alias call         call_fib
  end
end
