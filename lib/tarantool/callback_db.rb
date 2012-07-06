require 'tarantool/em_db'

module Tarantool
  class CallbackDB < EMDB
    class SpaceArray < ::Tarantool::SpaceArray
      alias by_pk        by_pk_blk
      alias all_by_key   all_by_key_blk
      alias first_by_key first_by_key_blk
      alias all_by_keys  all_by_keys_blk
      alias select       select_blk
      alias insert       insert_blk
      alias replace      replace_blk
      alias update       update_blk
      alias delete       delete_blk
      alias invoke       invoke_blk
      alias call         call_blk
      alias ping         ping_blk
    end

    class SpaceHash < ::Tarantool::SpaceHash
      alias by_pk   by_pk_blk
      alias all     all_blk
      alias first   first_blk
      alias select  select_blk
      alias insert  insert_blk
      alias replace replace_blk
      alias update  update_blk
      alias delete  delete_blk
      alias invoke  invoke_blk
      alias call    call_blk
      alias ping    ping_blk
    end

    class Query < ::Tarantool::Query
      alias select  select_blk
      alias all     all_blk
      alias first   first_blk
      alias insert  insert_blk
      alias replace replace_blk
      alias update  update_blk
      alias delete  delete_blk
      alias invoke  invoke_blk
      alias call    call_blk
      alias ping    ping_blk
    end
  end
end
