module Helpers
  module Truncate
    def teardown
      while (res = Tarantool.call(proc_name: 'box.select_range', args: [Tarantool.singleton_space.space_no.to_s, '0', '100'], return_tuple: true)) && res.tuples.size > 0
        res.tuples.each do |keys|
          Tarantool.delete key: keys.take(@primary_key_size || 1)
        end
      end
      super
    end
  end
end