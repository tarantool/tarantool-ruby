require_relative 'response'
require_relative 'consts'
module Tarantool16
  class SchemaSpace
    attr :sid, :name, :indices, :fields
    def initialize(sid, name, fields)
      @sid = sid
      @name = name
      @has_tail = false
      self.fields = fields
    end

    # imitate Option
    def ok?; true; end
    def data; self; end

    def fields=(flds)
      @field_names = {}
      @fields = []
      @has_tail = false
      flds.each_with_index do |fld, i|
        if @has_tail
          raise "no fields allowed after tail: #{flds}"
        end
        case fld
        when String, Symbol
          name = fld.to_s
          type = nil
        when Array
          name, type = fld
        when Hash
          name = fld['name'] || fld[:name]
          type = fld['type'] || fld[:type]
          tail = fld['tail'] || fld[:tail]
        end
        name_s = name.to_sym
        field = Field.new(name_s, i, type)
        @field_names[name] = field
        @field_names[name_s] = field
        @field_names[i] = field
        @fields << field
        @has_tail = true if tail
      end
      if @index_defs
        self.indices= @index_defs
      end
      flds
    end

    def indices=(inds)
      @index_defs = inds
      @index_names = {}
      @indices = []
      @_fields_2_ino = {}
      inds.each do |name, nom, type, parts|
        if @fields && @fields.size > parts.max
          part_names = parts.map{|p| @fields[p].name}
        else
          part_names = []
        end
        index = Index.new(name, nom, type, parts, part_names)
        @index_names[name] = index
        @index_names[name.to_sym] = index
        @index_names[nom] = index
        @indices[nom] = index
      end
    end

    def indices?
      @indices && !@indices.empty?
    end

    def get_ino(ino, key, iter, cb)
      if ino.nil?
        unless key.is_a?(Hash)
          opt = Option.error(SchemaError, "Could not detect index without field names and iterator: #{key.inspect} in #{name_sid}")
          return cb.call(opt)
        end
        unless iter.is_a?(Integer)
          iter = ::Tarantool16.iter(iter)
        end
        # key should be Hash here
        keys = key.keys
        _ino = @_fields_2_ino[keys]
        if _ino
          ind = @indices[_ino]
          return yield(_ino, ind.map_key(key))
        elsif _ino == false
          opt = Option.error(SchemaError, "Could not detect index for fields #{key.keys} in #{name_sid}")
          return cb.call(opt)
        end

        fields = keys.map{|fld|
          case fld
          when Integer
            fld
          when Symbol, String
            @field_names[fld].pos
          else
            return cb.call(Option.error(SchemaError, "Unknown field #{fld.inspect} in query key #{key.inspect}"))
          end
        }

        index = nil
        for ind in @indices
          next unless ind
          first_fields = ind.parts[0,fields.size]
          if ind.can_iterator?(iter)
            if fields == first_fields
              index = ind
              break
            elsif (fields - first_fields).empty?
              index = ind
            end
          end
        end
        if index
          @_fields_2_ino[keys.freeze] = index.pos
          yield index.pos, index.map_key(key)
        else
          @_fields_2_ino[keys.freeze] = false
          cb.call(Option.error(SchemaError, "Could not detect index for fields #{key.keys} in #{name_sid}"))
        end
      elsif index = @index_names[ino]
        yield index.pos, index.map_key(key)
      else
        cb.call(Option.error(SchemaError, "Could not find index #{ino} for fields #{Hash===key ? key.keys : key.inspect} in #{name_sid}"))
      end
    end

    def tuple2hash(ar)
      raise "No fields defined for #{name_sid}" unless @fields && !@fields.empty?
      res = {}
      i = 0
      flds = @fields
      s = flds.size - (@has_tail ? 1 : 0)
      while i < s
        res[flds[i].name] = ar[i]
        i += 1
      end
      if @has_tail
        tail = flds[s]
        unless tail.type.is_a?(Array)
          res[tail.name] = ar[s..-1]
        else
          res[tail.name] = ar[s..-1].each_slice(tail.type.size).to_a
        end
      end
      res
    end

    def name_sid
      @_np ||= "space #{@name}:#{@sid}"
    end

    def map_tuple(tuple)
      row = []
      unless @has_tail
        tuple.each_key do |k|
          field = @field_names[k]
          row[field.pos] = tuple[k]
        end
      else
        tail = @fields.last
        tuple.each do |k|
          field = @field_names[k]
          val = tuple[k]
          if field.equal? tail
            unless tail.type.is_a?(Array)
              row[field.pos,0] = val
            else
              row[field.pos,0] = val.flatten(1)
            end
          else
            row[field.pos] = tuple[k]
          end
        end
      end
      row
    end

    def map_ops(ops)
      ops.map do |op|
        case _1 = op[1]
        when Integer
          op
        when Symbol, String
          _op = op.dup
          _op[1] = @field_names[_1].pos
          _op
        when Array
          _1.dup.insert(1, @field_names[op[0]].pos)
        end
      end
    end

    def wrap_cb(cb)
      CallbackWrapper.new(self, cb)
    end

    class Field
      attr :name, :pos, :type
      def initialize(name, pos, type)
        @name = name
        @pos = pos
        @type = type
      end

      def to_s
        "<Fields #{@name}@#{pos}>"
      end
    end

    class Index
      attr :name, :pos, :parts, :type, :part_names, :part_positions
      ITERS = {
        tree:   (ITERATOR_EQ..ITERATOR_GT).freeze,
        hash:   [ITERATOR_ALL, ITERATOR_EQ].freeze,
        bitset: [ITERATOR_ALL, ITERATOR_EQ, ITERATOR_BITS_ALL_SET,
                 ITERATOR_BITS_ANY_SET, ITERATOR_BITS_ALL_NOT_SET].freeze,
        rtree:  [ITERATOR_ALL, ITERATOR_EQ, ITERATOR_GT, ITERATOR_GE, ITERATOR_LT, ITERATOR_LE,
                 ITERATOR_RTREE_OVERLAPS, ITERATOR_RTREE_NEIGHBOR].freeze
      }
      def initialize(name, pos, type, parts, part_names)
        @name = name
        @pos = pos
        @type = type.downcase.to_sym
        @iters = ITERS[@type] or raise "Unknown index type #{type.inspect}"
        @parts = parts
        @part_names = part_names
        @part_positions = {}
        parts.each_with_index{|p, i| @part_positions[p] = i}
        part_names.each_with_index{|p, i|
          @part_positions[p.to_s] = i
          @part_positions[p.to_sym] = i
        }
      end

      def can_iterator?(iter)
        @iters.include?(iter)
      end

      def map_key(key)
        return key if key.is_a?(Array)
        res = []
        positions = @part_positions
        key.each_key do |k|
          res[positions[k]] = key[k]
        end
        res
      end
    end

    class CallbackWrapper
      def initialize(space, cb)
        @space = space
        @cb = cb
      end

      def call(r)
        if r.ok?
          sp = @space
          r = Option.ok(r.data.map{|row| sp.tuple2hash(row)})
        end
        @cb.call r
      end
    end
  end
end
