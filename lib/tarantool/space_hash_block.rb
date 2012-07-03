require 'em-tarantool/space_hash'

module EM
  class Tarantool
    class SpaceHashBlock < SpaceHash
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
    end
  end
end
