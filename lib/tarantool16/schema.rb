require_relative 'response'
module Tarantool16
  class SchemaSpace
    attr :sid, :name, :indices, :fields
    def initialize(sid, name, fields, indices)
      @sid = sid
      @name = name
      @has_tail = false
      self.fields = fields
      self.indices = indices if indices
    end

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
    end

    def indices=(inds)
      @index_names = {}
      @indices = []
      @_fields_2_ino = {}
      inds.each_with_index do |ind, i|
        case ind
        when Array
          name, parts = ind
        else
          raise "Unknown index definition #{ind}"
        end
        parts = parts.map{|p|
          case p
          when Integer
            p
          when String, Symbol
            @field_names[p.to_s].pos or raise "Unknown field #{p} in index definition #{ind}"
          else
            raise "Unknown field #{p.inspect} in index definition #{ind.inspect}"
          end
        }
        part_names = parts.map{|p| @fields[p].name}
        index = Index.new(name, i, parts, part_names)
        @index_names[name] = index
        @index_names[name.to_sym] = index
        @index_names[i] = index
        @indices << index
      end
    end

    def indices?
      @indices && !@indices.empty?
    end

    def get_ino(ino, key, cb)
      if ino.nil?
        # key should be Hash here
        keys = key.keys
        unless (_ino = @_fields_2_ino[keys]).nil?
          return _ino ? yield(_ino) :
              cb.call(Option.error(
                SchemaError, "Could not detect index for fields #{key.keys} in #{name_sid}"))
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
        @indices.each do |ind|
          first_fields = ind.parts[0,fields.size]
          if fields == first_fields
            index = ind
            break
          elsif (fields - first_fields).empty?
            index = ind
          end
        end
        if index
          @_fields_2_ino[keys.freeze] = index.pos
          yield index.pos
        else
          @_fields_2_ino[keys.freeze] = false
          cb.call(Option.error(SchemaError, "Could not detect index for fields #{key.keys} in #{name_sid}"))
        end
      elsif index = @index_names[ino]
        yield index.pos
      else
        cb.call(Option.error(SchemaError, "Could not find index #{ino} for spacefor fields #{key.keys}"))
      end
    end

    def map_key(key, ino)
      res = []
      positions = @indices[ino].part_positions
      key.each_key do |k|
        res[positions[k]] = key[k]
      end
      res
    end

    def tuple2hash(ar)
      raise "No fields defined for #{name_sid}"
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
        tail_size = [*tail.type].size
        if tail_size == 1 || tail_size == 0
          res[tail.name] = ar[s..-1]
        else
          res[tail.name] = ar[s..-1].each_slice(tail_size).to_a
        end
      end
      res
    end

    def name_sid
      @_np ||= "space #{@name}:#{@sid}"
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
      attr :name, :pos, :parts, :part_names, :part_positions
      def initialize(name, pos, parts, part_names)
        @name = name
        @pos = pos
        @parts = parts
        @part_names = part_names
        @part_positions = {}
        parts.each_with_index{|p, i| @part_positions[p] = i}
        part_names.each_with_index{|p, i|
          @part_positions[p.to_s] = i
          @part_positions[p.to_sym] = i
        }
      end
    end
  end
end
