require_relative 'schema'
module Tarantool16
  class DB
    attr :conn

    def initialize(host, opts = {})
      @host = host
      @opts = opts
      @spaces = {}
      @space_futures = {}
      @defined_fields = {}
      @defined_indices = {}
      _fill_standard_spaces
      @conn = self.class::Connection.new(@host, opts)
    end

    def define_fields(sid, fields)
      sid = sid.to_s if Symbol === sid
      @defined_fields[sid] = fields
      if sp = @spaces[sid]
        if sp.sid && sp.name && !sp.name.empty?
          rf1 = @defined_fields[sp.sid]
          rf2 = @defined_fields[sp.name]
          if rf1 && rf2 && rf1 != rf2
            raise "Misconfigured defined fields for #{sp.name_sid}"
          end
        end
        sp.fields = fields
      end
    end

    def define_indices(sid, indices)
      sid = sid.to_s if Symbol === sid
      @defined_indices[sid] = indices
      if sp = @spaces[sid]
        if sp.sid && sp.name && !sp.name.empty?
          ri1 = @defined_indices[sp.sid]
          ri2 = @defined_indices[sp.name]
          if ri1 && ri2 && ri1 != ri2
            raise "Misconfigured defined indices for #{sp.name_sid}"
          end
        end
        sp.indices = indices
      end
    end

    def _fill_standard_spaces
      rf = @defined_fields
      rf[SPACE_SPACE] =
        [%w{id num}, %w{owner num}, %w{name str},
         %w{engine str}, %w{field_count num}, %w{flags str}, %w{format *}]
      rf[SPACE_INDEX] =
        [%w{sid num}, %w{iid num}, %w{name str},
         %w{type str}, %w{unique num}, %w{part_count num}, {name: 'parts', type: [:num, :str], tail: true}]
      ri = @defined_indices
      ri[SPACE_SPACE] = [ ['primary', [0]], ['owner', [1]], ['name', [2]] ]
      ri[SPACE_INDEX] = [ ['primary', [0, 1]], ['name', [0, 2]],  ]
      sp = @spaces
      sp[SPACE_SPACE] = SchemaSpace.new(SPACE_SPACE, :_space, rf[SPACE_SPACE], ri[SPACE_SPACE])
      sp[:_space] = sp["_space"] = sp[SPACE_SPACE]
      sp[SPACE_INDEX] = SchemaSpace.new(SPACE_INDEX, :_index, rf[SPACE_INDEX], ri[SPACE_INDEX])
      sp[:_index] = sp["_index"] = sp[SPACE_INDEX]
    end

    def _space(name)
      @spaces[name] || (Symbol === name && (sp = @spaces[name.to_s]) && (@spaces[name] = sp))
    end

    def _synchronized
      raise "Override #_synchronized"
    end

    UNDEF = Object.new.freeze
    def _space_future(name)
      _synchronized do
        if future = @space_futures[name]
          raise "Expected future for space, got #{future.kind}" unless future.kind == :space
          return future
        end
        future = self.class::Future.new(:space)
        @space_futures[name] = future
        case name
        when Symbol, String
          _select(SPACE_SPACE, INDEX_SPACE_NAME, [name], 0, 1, :==,
                  _fill_space_callback(name, future))
        when Integer
          _select(SPACE_SPACE, INDEX_SPACE_PRIMARY, [name], 0, 1, :==,
                  _fill_space_callback(name, future))
        end
        future
      end
    end

    def _fill_space_callback(name, future)
      lambda do |r|
        _synchronized do
          unless (d = @space_futures.delete(name)).equal?(future)
            raise "Future doesn't match stored future: #{d.inspect} != #{future.inspect}"
          end
          return future.set(r) unless r.ok?
          if r.data.empty?
            return future.set(Option.error(SchemaError, "Space #{name} not found"))
          end
          r = r.data[0]
          if sp = @spaces[r[0]]
            sp.name = r[2]
            fields = @defined_fields[r[0]] || @defined_fields[r[2]] || r[6]
            sp.fields = fields
          else
            fields = @defined_fields[r[0]] || @defined_fields[r[2]] || r[6]
            indices = @defined_indices[r[0]] || @defined_indices[r[2]]
            sp = SchemaSpace.new(r[0], r[2], fields, indices)
            @spaces[r[0]] = sp
          end
          if r[6] && !r[6].empty? && !sp.fields
            sp._setup_fetched_fields(r[6])
          end
          @spaces[sp.name] = sp
          @spaces[sp.name.to_sym] = sp
          future.set Option.ok(sp)
        end
      end
    end

    def _fill_space(name, cb)
      if sp = _space(name)
        yield sp
      else
        _space_future(name).then(lambda do |r|
          return cb.call(r) unless r.ok?
          yield r.data
        end)
      end
    end

    def _index_future(sid)
      _synchronized do
        if future = @space_futures[sid]
          raise "Expected future for space, got #{future.kind}" unless future.kind == :index
          return future
        end
        future = self.class::Future.new(:index)
        @space_futures[sid] = future
        fill_indices = lambda do |r|
          return future.set(r) unless r.ok?
          if r.data.empty?
            return cb.call(Option.error(SchemaError, "no indeces found for #{sp.name_sid}"))
          end
          sp = _space(sid)
          sp.indices = r.data.map{|row|
            [row[2], 6.step(row.size-1, 2).map{|i| row[i]}]
          }
          future.set(Option.ok(sp))
        end
        _select(SPACE_INDEX, INDEX_INDEX_PRIMARY, [sid], 0, 10, :==, fill_indices)
        future
      end
    end

    def _fill_indices(sp, ino, key, cb)
      return yield ino if Integer === ino
      return yield 0 if ino.nil? && Array === key
      raise "Key could be an Array or Hash, got #{key.inspect}" unless Hash === key
      unless sp.indices?
        inds = @defined_indices[sp.sid] || @defined_indices[sp.name]
        sp.indices = inds if inds
      end
      if sp.indices?
        sp.get_ino(ino, key, cb){|_ino| yield _ino}
      else
        _index_future(sp.sid).then(lambda do |r|
          cb.call(r) unless r.ok?
          sp = r.data
          sp.get_ino(ino, key, cb){|_ino| yield _ino}
        end)
      end
    end

    def _insert(sno, tuple, cb)
      if Integer === sno && Array === tuple
        return conn._insert(sno, tuple, cb)
      end
      _fill_space(sno, cb) do |sp|
        tuple = sp.map_tuple(tuple)  if Hash === tuple
        conn._insert(sp.sid, tuple, cb)
      end
    end

    def _replace(sno, tuple, cb)
      if Integer === sno && Array === tuple
        return conn._replace(sno, tuple, cb)
      end
      _fill_space(sno, cb) do |sp|
        tuple = sp.map_tuple(tuple)  if Hash === tuple
        conn.insert(sp.sid, tuple, cb)
      end
    end

    def _delete(sno, ino, key, cb)
      ino = 0 if ino.nil? && Array === key
      if Integer === sno && Integer === ino && Array === key
        return conn._delete(sno, ino, key, cb)
      end
      _fill_space(sno, cb) do |sp|
        _fill_indices(sp, ino, key, cb) do |_ino|
          key = sp.map_key(key, _ino) if Hash === key
          conn._delete(sp.sid, _ino, key, cb)
        end
      end
    end

    def _select(sno, ino, key, offset, limit, iterator, cb)
      key = [] if key.nil?
      ino = 0 if ino.nil? && Array === key
      if Integer === sno && Integer === ino && (Array === key || key.nil?)
        return conn._select(sno, ino, key, offset, limit, iterator, cb)
      end
      _fill_space(sno, cb) do |sp|
        _fill_indices(sp, ino, key, cb) do |_ino|
          key = sp.map_key(key, _ino) if Hash === key
          conn._select(sp.sid, _ino, key, offset, limit, iterator, cb)
        end
      end
    end

    def _update(sno, ino, key, ops, cb)
      ino = 0 if ino.nil? && Array === key
      if Integer === sno && Integer === ino && Array == key &&
          Array === ops && ops.all?{|a| Integer === ops[1]}
        return conn._update(sno, ino, key, ops, cb)
      end
      _fill_space(sno, cb) do |sp|
        _fill_indices(sp, ino, key, cb) do |_ino|
          key = sp.map_key(key, _ino) if Hash === key
          ops = sp.map_ops(ops)
          conn._select(sp.sid, _ino, key, offset, limit, iterator, cb)
        end
      end
    end

    def _call(name, args, cb)
      conn._call(name, args, cb)
    end

    def _ping(cb)
      conn._ping(cb)
    end
  end
end
