require 'tarantool'
require 'tarantool/record/select'
require 'tarantool/core-ext'

module Tarantool
  class RecordError < StandardError; end
  class UpdateNewRecord < RecordError; end

  class BaseRecord
    extend ::Tarantool::ClassAttribute
    t_class_attribute :fields
    self.fields = {}.freeze

    t_class_attribute :default_values
    self.default_values = {}.freeze

    t_class_attribute :indexes
    self.indexes = [].freeze

    t_class_attribute :_space_no
    t_class_attribute :_tarantool

    t_class_attribute :_shard_proc
    t_class_attribute :_shard_fields
    self._shard_proc = nil
    self._shard_fields = nil

    class << self
      alias set_shard_proc _shard_proc=
    end

    module ClassMethods
      def tarantool(v=nil)
        unless v
          _tarantool
        else
          self.tarantool = v
        end
      end

      def tarantool=(v)
        reset_space!
        unless ::Tarantool::DB === v && v.primary_interface == :synchronous
          raise ArgumentError, "you may assing to record's tarantool only instances of Tarantool::BlockDB or Tarantool::FiberDB"
        end
        self._tarantool= v
      end
      alias set_tarantool tarantool=

      def space_no(v=nil)
        unless v
          _space_no
        else
          self.space_no = v
        end
      end

      def space_no=(v)
        reset_space!
        self._space_no = v
      end
      alias set_space_no  space_no=

      def field(name, type, params = {})
        type = Serializers.check_type(type)

        raise ArgumentError, "_tail should be last declaration"  if fields.include?(:_tail)
        self.fields = fields.merge(name => type)
        index name  if indexes.empty?

        if params[:default]
          self.default_values = default_values.merge name => params[:default]
        end

        define_field_accessor(name, type)
      end

      def _tail(*types)
        types = types.map{|type| Serializers.check_type(type)}

        raise ArgumentError, "double _tail declaration"  if fields.include?(:_tail)
        self.fields = fields.merge(:_tail => types)

        define_field_accessor(:_tail, types)
      end

      def index(*fields)
        options = Hash === fields.last ? fields.pop : {}
        if options[:primary]
          self.indexes = indexes.dup.tap{|ind| ind[0] = fields}
        else
          self.indexes += [fields]
        end
      end

      def shard_proc(cb = nil, &block)
        if cb ||= block
          self._shard_proc = cb
        else
          _shard_proc
        end
      end

      def shard_fields(*args)
        if args.empty?
          _shard_fields
        else
          self._shard_fields = args
        end
      end
      alias set_shard_fields shard_fields

      def primary_index
        indexes[0]
      end

      def space
        @space ||= begin
            shard_fields = _shard_fields || primary_index
            shard_proc = _shard_proc ||
              if shard_fields.size == 1
                case fields[shard_fields[0]]
                when :int, :int16, :int8
                  :sumbur_murmur_fmix
                when :int64
                  :sumbur_murmur_int64
                when :string
                  :sumbur_murmur_str
                else
                  :default
                end
              else
                :default
              end
            _tarantool.space_hash(_space_no, fields.dup,
                                 keys: indexes,
                                 shard_fields: shard_fields,
                                 shard_proc: shard_proc
                                )
          end
      end

      # space that will return records as results for calls
      # it is useful, if you wish to use callback interface
      def auto_space
        @auto_space ||= begin
            space.with_translator(method(:from_fetched))
          end
      end

      def reset_space!
        @space = nil
        @auto_space = nil
      end

      def by_pk(pk)
        if Hash === (res = space.by_pk(pk))
          from_fetched(res)
        end
      end

      def by_pks(pks)
        space.all_by_pks(pks).map{|hash| from_fetched(hash)}
      end

      def find(*args)
        if args.size == 1
          by_pk(args[0])
        else
          by_pks(args)
        end
      end

      def first(cond)
        if Hash === cond
          if Hash === (res = space.first(cond))
            from_fetched(res)
          end
        else
          by_pk(cond)
        end
      end

      def all(cond, opts={})
        res = space.all(cond, opts)
        res.map!{|hash| from_fetched(hash)}
        res
      end

      def select(cond=nil, opts={})
        cond.nil? ? Select.new(self) : all(cond, opts)
      end

      def create(attributes = {})
        r = new(attributes)
        r.save
        r
      end

      # Call stored procedure without returning tuples. By default, it prepends
      # +space_no+ to arguments. To avoid prepending, set +space_no: nil+ in options.
      #
      #   MyRecord.call('box.select_range', offset, limit)
      #   MyRecord.call('myfunction', arg1, arg2, space_no: nil)
      #
      def invoke(proc_name, *args)
        opts = Hash === args.last ? args.pop : {}
        space.invoke(proc_name, args, opts)
      end

      # Call stored procedure. By default, it prepends +space_no+ to arguments.
      # To avoid prepending, set +space_no: nil+ in options.
      #
      #   MyRecord.call('box.select_range', offset, limit)
      #   MyRecord.call('myfunction', arg1, arg2, space_no: nil)
      #
      # You could recieve arbitarry arrays or hashes instead of instances of
      # record, if you pass +:returns+ argument. See documentation for +SpaceHash+
      # for this.
      def call(proc_name, *args)
        opts = Hash === args.last ? args.pop : {}
        res = space.call(proc_name, args, opts)
        if Array === res && !opts[:returns]
          res.map{|hash| from_fetched(hash) }
        else
          res
        end
      end

      def from_fetched(hash)
        hash && allocate.__fetched(hash)
      end

      def insert(hash, ret_tuple = false)
        hash = default_values.merge(hash)
        if ret_tuple
          from_fetched space.insert(hash, return_tuple: true)
        else
          space.insert(hash)
        end
      end

      def replace(hash, ret_tuple = false)
        hash = default_values.merge(hash)
        if ret_tuple
          from_fetched space.replace(hash, return_tuple: true)
        else
          space.replace(hash)
        end
      end

      def store(hash, ret_tuple = false)
        hash = default_values.merge(hash)
        if ret_tuple
          from_fetched space.store(hash, return_tuple: true)
        else
          space.store(hash)
        end
      end

      def update(pk, ops, ret_tuple=false)
        if ret_tuple
          from_fetched space.update(pk, ops, return_tuple: true)
        else
          space.update(pk, ops)
        end
      end

      def delete(pk, ret_tuple=false)
        if ret_tuple
          from_fetched space.delete(pk, return_tuple: true)
        else
          space.delete(pk)
        end
      end

      %w{where limit offset shard}.each do |meth|
        class_eval <<-"EOF", __FILE__, __LINE__
          def #{meth}(arg)
            select.#{meth}(arg)
          end
        EOF
      end
    end
    extend ClassMethods

    module InstanceMethods
      attr_accessor :__new_record
      attr_reader :attributes
      alias new_record __new_record
      alias new_record? new_record

      def new_record!
        @__new_record = true
        self
      end

      def old_record!
        @__new_record = false
        self
      end

      def set_attributes(attributes)
        attributes.each do |k, v|
          send("#{k}=", v)
        end      
      end

      def _tail
        @attributes[:_tail]
      end

      def _tail=(v)
        @attributes[:_tail] = v
      end

      def update_attributes(attributes)
        set_attributes(attributes)
        save
      end

      def id
        (primary = self.class.primary_index).size == 1 ?
          @attributes[primary[0]] :
          @attributes.values_at(*primary)
      end

      def space
        self.class.space
      end

      def auto_space
        self.class.space
      end

      def reload
        if hash = space.by_pk(id)
          @__new_record = false
          @attributes = hash
          self
        else
          _raise_doesnt_exists("reload")
        end
      end

      def ==(other)
        equal?(other) || (other.class == self.class && id == other.id)
      end

      # update record in db first, reload it then
      #
      #   record.update({:state => 'sleep', :sleep_count => [:+, 1]})
      #   record.update([[:state, 'sleep'], [:sleep_count, :+, 1]])
      def update(ops)
        raise UpdateNewRecord, "Could not call update on new record"  if @__new_record
        unless new_attrs = space.update(id, ops, return_tuple: true)
          _raise_doesnt_exists
        end
        @attributes = new_attrs

        self
      end

      def increment(field, by = 1)
        update([[field.to_sym, :+, by]])
      end

      def _raise_doesnt_exists(action = "update")
        raise TupleDoesntExists.new(0x3102, "Record which you wish to #{action}, doesn't exists")
      end
    end
    include InstanceMethods

  end
end
