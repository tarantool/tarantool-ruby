module Helpers
  module Truncate
    def teardown
      while (res = space.call('box.select_range', space.space_no, 0, 100, return_tuple: true)) && res.tuples.size > 0
        res.tuples.each do |keys|
          space.delete key: keys.take(@primary_key_size || 1)
        end
      end
      super
    end
  end
end