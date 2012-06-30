require 'em-tarantool/space_base'

module EM
  class Tarantool
    class SpaceCB < SpaceBase
      alias by_pk        by_pk_cb
      alias all_by_key   all_by_key_cb
      alias first_by_key first_by_key_cb
      alias all_by_keys  all_by_keys_cb
      alias select       select_cb
      alias insert       insert_cb
      alias replace      replace_cb
      alias update       update_cb
      alias delete       delete_cb
      alias invoke       invoke_cb
      alias call         call_cb
    end
  end
end
