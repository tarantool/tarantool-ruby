module Helpers
  module Truncate
    def teardown       
      while (res = space.call('box.select_range', space.space_no, 0, 100, return_tuple: true)) && res.tuples.size > 0
        res.tuples.each do |k, *_|
          space.delete key: k
        end
      end
      super
    end
  end
end