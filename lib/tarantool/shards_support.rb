module Tarantool
  module Request
    def _init_shard_vars
      @shards_count = @tarantool.shards_count
      @default_shard = 0
      @_shard_callback = nil
    end

    def _detect_shards_by_positions(keys, pos)
      pos.map!{|i| i || 2**30}
      _flat_uniq keys.map{|key| detect_shard(key.values_at(*pos)) }
    end

    def _detect_shards(keys)
      _flat_uniq keys.map{|key| detect_shard(key)}
    end

    def _flat_uniq(array)
      hsh = {}
      array.each do |v|
        if v.respond_to?(:each)
          v.each{|vv| hsh[vv] = true}
        else
          hsh[v] = true
        end
      end
      hsh.keys
    end

    # methods for override
    def detect_shard(pk)
      if @_shard_callback
        @_shard_callback.call(pk, @shards_count, self)
      elsif Array === pk
        if pk.size == 1 && Integer === pk[0]
          pk[0] % @shards_count
        elsif pk.all?
          pk.hash % @shards_count
        else
          all_shards
        end
      elsif Integer === pk
        pk % @shards_count
      else
        [pk].hash % @shards_count
      end
    end

    def _get_shard_nums
      @shards_count == 1 ? @default_shard : yield
    end

    def all_shards
      (0...@shards_count).to_a
    end

    def shard(shard_number)
      if shard_number >= @shards_count
        raise ArgumentError, "There is no shard #{shard_number}, amount of shards is #{@shards_count}"
      end
      (@_fixed_shards ||= {})[shard_number] ||=
          clone.instance_exec do
            @shards_count = 1
            @default_shard = shard_number
            self
          end
    end

    def shard_by(callback = nil, &block)
      clone.instance_exec do
        @_shard_callback = callback || block
        self
      end
    end
  end
end
