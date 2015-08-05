require_relative 'schema'
module Tarantool16
  class DB
    attr :conn

    def initialize(host, opts = {})
      @host = host
      @opts = opts.dup
      @future = nil
      @spaces = nil
      @defined_fields = {}
      _fill_standard_spaces
      @conn = self.class::Connection.new(@host, @opts)
    end

    def define_fields(sid, fields)
      sid = sid.to_s if sid.is_a?(Symbol)
      @defined_fields[sid] = fields
      if @spaces && (sp = @spaces[sid])
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

    def _fill_standard_spaces
      rf = @defined_fields
      rf[SPACE_INDEX] =
        [%w{sid num}, %w{iid num}, %w{name str},
         %w{type str}, %w{unique num}, %w{part_count num},
         {name: 'parts', type: [:num, :str], tail: true}]
    end

    def _synchronized
      raise "Override #_synchronized"
    end

    UNDEF = Object.new.freeze
    def _with_space(name, cb)
      future = @future || _space_future
      future.then_blk do |r|
        unless r.ok?
          cb.call r
        else
          sps = r.data
          sp = sps[name]
          if sp.nil? && Symbol == name
            sp = sps[name.to_s]
            sps[name] = sp unless sp.nil?
          end
          if sp.nil?
            cb.call Option.error(SchemaError, "space #{name} not found")
          else
            yield sp
          end
        end
      end
    end

    def _space_future
      _synchronized do
        return @future if @future
        future = @future = self.class::SchemaFuture.new
        fill_indexes = nil
        spaces = nil
        fill_spaces = lambda do|r|
          unless r.ok?
            future.set r
            _synchronized do
              @future = nil
            end
          else
            _synchronized do
              _fill_spaces(r.data)
              spaces = @spaces
              _select(SPACE_INDEX, 0, [], 0, 2**30, :all, false, fill_indexes)
            end
          end
        end
        fill_indexes = lambda do |r|
          unless r.ok?
            future.set r
            _synchronized do
              @future = nil
              @spaces = nil
            end
          else
            _synchronized do
              _fill_indices(spaces, r.data)
              future.set Option.ok(spaces)
            end
          end
        end
        _select(SPACE_SPACE, 0, [], 0, 2**30, :all, false, fill_spaces)
        return future
      end
    end

    def _fill_spaces(rows)
      @spaces = {}
      rows.each do |row|
        fields = @defined_fields[row[0]] || @defined_fields[row[2]] || row[6]
        sp = SchemaSpace.new(row[0], row[2], fields)
        @spaces[row[0]] = sp
        @spaces[sp.name] = sp
        @spaces[sp.name.to_sym] = sp
      end
    end

    def _fill_indices(spaces, rows)
      rows.
        map{|row| [row[0], [row[2], row[1], row[3], 6.step(row.size-1, 2).map{|i| row[i]}]]}.
        group_by{|sid, _| sid}.
        each do |sid, inds|
          sp = spaces[sid]
          sp.indices = inds.map{|_sid, ind| ind}
        end 
    end

    def _insert(sno, tuple, need_hash, cb)
      if !need_hash && sno.is_a?(Integer) && tuple.is_a?(Array)
        return conn._insert(sno, tuple, cb)
      end
      _with_space(sno, cb) do |sp|
        _tuple = tuple.is_a?(Hash) ? sp.map_tuple(tuple) : tuple
        _cb = need_hash ? sp.wrap_cb(cb) : cb
        conn._insert(sp.sid, _tuple, _cb)
      end
    end

    def _replace(sno, tuple, need_hash, cb)
      if !need_hash && sno.is_a?(Integer) && tuple.is_a?(Array)
        return conn._replace(sno, tuple, cb)
      end
      _with_space(sno, cb) do |sp|
        _tuple = tuple.is_a?(Hash) ? sp.map_tuple(tuple) : tuple
        _cb = need_hash ? sp.wrap_cb(cb) : cb
        conn._replace(sp.sid, _tuple, _cb)
      end
    end

    def _delete(sno, ino, key, need_hash, cb)
      ino = 0 if ino.nil? && key.is_a?(Array)
      if !need_hash && sno.is_a?(Integer) && ino.is_a?(Integer) && key.is_a?(Array)
        return conn._delete(sno, ino, key, cb)
      end
      _with_space(sno, cb) do |sp|
        sp.get_ino(ino, key, ITERATOR_EQ, cb) do |_ino, _key|
          _cb = need_hash ? sp.wrap_cb(cb) : cb
          conn._delete(sp.sid, _ino, _key, _cb)
        end
      end
    end

    def _select(sno, ino, key, offset, limit, iterator, need_hash, cb)
      key = [] if key.nil?
      ino = 0 if ino.nil? && key.is_a?(Array)
      unless iterator.is_a?(Integer)
        if key.empty? && (Array === key || Hash === key)
          iterator = ITERATOR_ALL
        else
          iterator = ::Tarantool16.iter(iterator)
        end
      end
      if sno.is_a?(Integer) && ino.is_a?(Integer) && (key.is_a?(Array) || key.nil?)
        return conn._select(sno, ino, key, offset, limit, iterator, cb)
      end
      _with_space(sno, cb) do |sp|
        sp.get_ino(ino, key, iterator, cb) do |_ino, _key|
          _cb = need_hash ? sp.wrap_cb(cb) : cb
          conn._select(sp.sid, _ino, _key, offset, limit, iterator, _cb)
        end
      end
    end

    def _update(sno, ino, key, ops, need_hash, cb)
      ino = 0 if ino.nil? && key.is_a?(Array)
      ops_good = ops.is_a?(Array) && ops.all?{|a| ops[1].is_a?(Integer)}
      if sno.is_a?(Integer) && ino.is_a?(Integer) && key.is_a?(Array) && ops_good
        return conn._update(sno, ino, key, ops, cb)
      end
      _with_space(sno, cb) do |sp|
        sp.get_ino(ino, key, ITERATOR_EQ, cb) do |_ino, _key|
          _ops = ops_good ? ops : sp.map_ops(ops)
          _cb = need_hash ? sp.wrap_cb(cb) : cb
          conn._update(sp.sid, _ino, _key, _ops, _cb)
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
