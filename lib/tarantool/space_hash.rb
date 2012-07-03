require 'tarantool/util'
require 'tarantool/request'
require 'tarantool/response'
require 'tarantool/core-ext'

module Tarantool
  class SpaceHash
    include Request

    def initialize(tarantool, space_no, fields_def, primary_index, indexes)
      @tarantool = tarantool
      @space_no = space_no

      field_to_pos = {}
      field_to_type = {}
      field_types = []
      i = 0
      last_type = nil
      for k, t in fields_def
        k = k.to_sym
        unless k == :_tail
          raise ValueError, ":_tail field should be defined last"  if field_to_pos[:_tail]
          last_type = t = t.to_sym
          field_to_pos[k] = i
          field_to_type[k] = t
          field_types << t
        else
          t = Array(t).map{|tt| tt.to_sym}
          field_to_pos[:_tail] = i
          field_to_type[:_tail] = t
          field_types.concat t
          field_types << t.size
        end
        i += 1
      end
      field_to_pos[:_tail] ||= i
      field_to_type[:_tail] ||= [last_type]
      @field_to_pos = field_to_pos
      @field_to_type = field_to_type
      @field_names = field_to_pos.keys
      @field_types = field_types
      @tail_pos  = field_to_pos[:_tail]
      @tail_size = field_to_type[:_tail].size

      indexes = Array(indexes).map{|ind| Array(ind)}
      primary_index = primary_index ?
                      Array(primary_index) :
                      [@field_names.first]
      @index_fields = [primary_index].concat(indexes).map{|ind| ind.map{|fld| fld.to_sym}}
      @indexes = _map_indexes(@index_fields)
      @translators = [TranslateToHash.new(@field_names, @tail_size)].freeze
    end

    def _map_indexes(indexes)
      indexes.map do |index|
        index.map do |name|
          @field_to_type[name.to_sym] or raise "Wrong index field name: #{index} #{name}"
        end << :error
      end
    end

    def _send_request(type, body, cb)
      @tarantool._send_request(type, body, cb)
    end

    def select_cb(keys, offset, limit, cb)
      keys = Hash === keys ? [keys] : Array(keys)
      index_names = keys.first.keys
      index_no = @index_fields.index{|fields|
        fields.take(index_names.size) == index_names
      }
      unless index_no
        index_names.sort!
        index_no = @index_fields.index{|fields|
          fields.take(index_names.size).sort == index_names
        }
        raise ValueError, "Could not find index for keys #{index_names}"  unless index_no
        index_names = @index_fields[index_no].take(index_names.size)
      end
      index_types = @indexes[index_no]

      unless Array === keys.first.first.last
        keys = keys.map{|key| key.values_at(*index_names)}
      else
        keys = keys.first.values_at(*index_names).transpose
      end

      _select(@space_no, index_no, offset, limit, keys, cb, @field_types, index_types, @translators)
    end

    def all_cb(keys, cb, opts = {})
      select_cb(keys, opts[:offset] || 0, opts[:limit] || -1, cb)
    end

    def first_cb(key, cb)
      select_cb(key, 0, :first, cb)
    end

    def by_pk_cb(key_array, cb)
      key_array = _prepare_pk(key_array)
      _select(@space_no, 0, 0, :first, [key_array], cb, @field_types, @indexes[0], @translators)
    end

    def _prepare_tuple(tuple)
      unless (exc = (tuple.keys - @field_names)).empty?
        raise ValueError, "wrong keys #{exc} for tuple"
      end
      tuple_ar = @field_names.map{|fld| tuple[fld] }
      tuple_ar.flatten!
      tuple_ar
    end

    def insert_cb(tuple, cb, opts = {})
      _insert(@space_no, BOX_ADD, _prepare_tuple(tuple),
              @field_types, cb, opts[:return_tuple], @translators)
    end

    def replace_cb(tuple, cb, opts = {})
      _insert(@space_no, BOX_REPLACE, _prepare_tuple(tuple),
              @field_types, cb, opts[:return_tuple], @translators)
    end

    def _prepare_pk(pk)
      if Hash === pk
        pk_fields = pk.keys
        unless (pindex = @index_fields[0]) == pk_fields
          if !(exc = (pk_fields - pindex)).empty?
            raise ValueError, "Wrong keys #{exc} for primary index"
          elsif !(exc = (pindex - pk_fields)).empty?
            raise ValueError, "you should provide values for all keys of primary index (missed #{exc})"
          end
        end
        pk.values_at *pindex
      else
        Array(pk)
      end
    end

    def update_cb(pk, operations, cb, opts = {})
      pk = _prepare_pk(pk)
      opers = []
      operations.each{|oper|
        if Array === oper[0]
          oper = oper[1..-1].unshift(*oper[0])
        end
        case oper[0]
        when Integer
          opers << oper[1..-1].unshift(oper[0] + @tail_pos)
        when :_tail
          raise ValueError, "_tail update should be array with operations" unless Array === oper[1]
          oper[1].each_with_index{|op, i| opers << [i + @tail_pos, op]}
        else
          opers << oper[1..-1].unshift(
            @field_to_pos[oper[0]]  || raise(ValueError, "Not defined field name #{oper[0]}")
          )
        end
      }
      _update(@space_no, pk, opers, @field_types,
              @indexes[0], cb, opts[:return_tuple], @translators)
    end

    def delete_cb(pk, cb, opts = {})
      _delete(@space_no, _prepare_pk(pk), @field_types,
              @indexes[0], cb, opts[:return_tuple], @translators)
    end

    def invoke_cb(func_name, values, cb, opts = {})
      values, opts = _space_call_fix_values(values, @space_no, opts)
      _call(func_name, values, cb, opts)
    end

    def call_cb(func_name, values, cb, opts = {})
      values, opts = _space_call_fix_values(values, @space_no, opts)

      opts[:return_tuple] = true  if opts[:return_tuple].nil?
      if opts[:return_tuple]
        opts[:returns] ||= @field_to_type
        if Hash === opts[:returns]
          opts[:returns], *opts[:translators] = 
            _parse_hash_definition(opts[:returns])
        end
      end
      
      _call(func_name, values, cb, opts)
    end

    include CommonSpaceBlockMethods
    # callback with block api
    def by_pk_blk(key_array, &block)
      by_pk_cb(key_array, block)
    end

    def all_blk(keys, opts = {}, &block)
      all_cb(keys, block, opts)
    end

    def first_blk(key, &block)
      first_blk(key, block)
    end

    def select_blk(keys, offset, limit, &block)
      select_cb(keys, offset, limit, block)
    end
  end
end
