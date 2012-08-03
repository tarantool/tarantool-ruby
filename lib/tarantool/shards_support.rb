require 'sumbur'
require 'murmurhash3'
module Tarantool
  module Request
    class SumburMurmur32
      include Sumbur
      include MurmurHash3::V32
    end

    class SumburMurmurFmix < SumburMurmur32
      def call(shard_values, shards_count, this)
        shard_values[0] &&
          sumbur(murmur3_32_fmix(shard_values[0]), shards_count)
      end
    end

    class SumburMurmurInt64 < SumburMurmur32
      def call(shard_values, shards_count, this)
        shard_values[0] &&
          sumbur(murmur3_32_int64_hash(shard_values[0]), shards_count)
      end
    end

    class SumburMurmurStr < SumburMurmur32
      def call(shard_values, shards_count, this)
        shard_values[0] &&
          sumbur(murmur3_32_str_hash(shard_values[0]), shards_count)
      end
    end

    class DefaultShardProc < SumburMurmur32
      def call(shard_values, shards_count, this)
        if shard_values.all?
          hash = i = 0
          size = shard_values.size
          while i < size
            hash = case value = shard_values[i]
              when Integer
                murmur3_32_int64_hash(value, hash)
              when String
                murmur3_32_str_hash(value, hash)
              else
                raise ValueError, "Default sharding proc could deal only with strings and integers"
              end
            i += 1
          end
          sumbur(hash, shards_count)
        end
      end
    end

    class ModuloShardProc
      def call(shard_values, shards_count, this)
        shard_values[0] && shard_values[0] % shards_count
      end
    end

    attr_reader :shard_proc, :shards_count, :previous_shards_count
    attr_reader :insert_with_shards_count

    def _init_shard_vars(shard_proc, init_shard_for_index = true)
      if init_shard_for_index
        @shard_by_index = @index_fields.index{|index| index == @shard_fields}
        @shard_for_index = @index_fields.map{|index|
            if (pos = @shard_fields.map{|name| index.index(name)}).any?
              pos.map{|i| i || 2**30}
            end
        }
        @shard_proc = case shard_proc
                      when nil, :default
                        DefaultShardProc.new
                      when :sumbur_murmur_fmix
                        SumburMurmurFmix.new
                      when :sumbur_murmur_int64
                        SumburMurmurInt64.new
                      when :sumbur_murmur_str
                        SumburMurmurStr.new
                      when :modulo, :module
                        ModuloShardProc.new
                      else
                        unless shard_proc.respond_to?(:call)
                          raise ArgumentError, "Wrong sharding proc object #{shard_proc.inspect}"
                        end
                        shard_proc
                      end
      end

      @shards_count = @tarantool.shards_count
      @previous_shards_count = @tarantool.previous_shards_count
      @insert_with_shards_count = @tarantool.insert_with_shards_count
      @default_shard = 0
    end

    def _detect_shards_for_keys(keys, index_no)
      return _detect_shards_for_key(keys, index_no)  unless Array === keys
      if index_no == @shard_by_index && (
            @index_fields.size == 1 ||
            keys.all?{|key| Array === key && key.size == @index_fields.size}
          )
        _flat_uniq keys.map{|key| _detect_shard(key)}
      elsif pos = @shard_for_index[index_no]
        _flat_uniq keys.map{|key| _detect_shard([*key].values_at(*pos)) }
      else
        _all_shards
      end
    end

    def _detect_shards_for_key(key, index_no)
      if index_no == @shard_by_index
        _detect_shard(key)
      elsif pos = @shard_for_index[index_no]
        _detect_shard(key.values_at(*pos))
      else
        _all_shards
      end
    end

    def _detect_shards(keys)
      _flat_uniq keys.map{|key| _detect_shard(key)}
    end

    def _detect_shards_for_insert(keys)
      _flat_uniq keys.map{|key| _detect_shard_for_insert(key)}
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

    def _detect_shard_for_insert(shard_values)
      shard_values = [shard_values]  unless Array === shard_values
      @shard_proc.call(shard_values, @insert_with_shards_count, self) || _all_shards
    end

    def _detect_shard(shard_values)
      shard_values = [shard_values]  unless Array === shard_values
      shards = @shard_proc.call(shard_values, @shards_count, self) || _all_shards
      if @previous_shards_count
        prev_shards = @shard_proc.call(shard_values, @previous_shards_count, self) || _all_shards
        shards = [*shards, *prev_shards].uniq
      end
      shards
    end

    def _get_shard_nums
      @shards_count == 1 ? @default_shard : yield
    end

    def _all_shards
      (0...@shards_count).to_a
    end

    def detect_shard(shard_values)
      @shards_count == 1 ? @default_shard : _detect_shard(shard_values)
    end

    def detect_shard_for_insert(shard_values)
      @shards_count == 1 ? @default_shard : _detect_shard_for_insert(shard_values)
    end

    def detect_shards(shard_values)
      @shards_count == 1 ? @default_shard : _detect_shards(shard_values)
    end

    def detect_shards_for_insert(shard_values)
      @shards_count == 1 ? @default_shard : _detect_shards_for_insert(shard_values)
    end

    def all_shards
      @shards_count == 1 ? @default_shard : (0...@shards_count).to_a
    end

    def shard(shard_number)
      case shard_number
      when Integer
        if shard_number >= @shards_count
          raise ArgumentError, "There is no shard #{shard_number}, amount of shards is #{@shards_count}"
        end
      when Array
        shard_number.each do|i|
          if i >= @shards_count
            raise ArgumentError, "There is no shard #{i}, amount of shards is #{@shards_count}"
          end
        end
      else
        raise ArgumentError, "Shard number should be integer or array of integers"
      end
      (@_fixed_shards ||= {})[shard_number] ||=
          clone.instance_exec do
            @shards_count = 1
            @default_shard = shard_number
            self
          end
    end
  end
end
