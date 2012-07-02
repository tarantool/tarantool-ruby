require "em-tarantool/space_hash"

module EM
  class Tarantool
    class SpaceHashFiber < SpaceHash
      alias by_pk   by_pk_fib
      alias all     all_fib
      alias first   first_fib
      alias select  select_fib
      alias insert  insert_fib
      alias replace replace_fib
      alias update  update_fib
      alias delete  delete_fib
      alias invoke  invoke_fib
      alias call    call_fib
    end
  end
end
