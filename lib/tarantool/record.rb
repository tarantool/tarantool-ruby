require 'active_model'
require 'tarantool'
class Tarantool
  class Select
    include Enumerable
    attr_reader :record
    def initialize(record)
      @record = record
    end

    def space_no
      @record.space_no
    end

    def each(&blk)
      res = to_records @record.space.select(*@tuples, index_no: @index_no, limit: @limit, offset: @offset).tuples
      res.each &blk
    end

    def call(proc_name, *args)
      to_records @record.space.call(proc_name, *args, return_tuple: true).tuples
    end

    def limit(limit)
      @limit = limit
      self
    end

    def offset(offset)
      @offset = offset
      self
    end

    # id: 1
    # id: [1, 2, 3]
    # [{ name: 'a', email: 'a'}, { name: 'b', email: 'b'}]
    def where(params)
      raise SelectError.new('Where condition already setted') if @index_no # todo?
      keys = case params
        when Hash
          params.keys
        when Array
          params.first.keys
        end

      unless @index_no = detect_index_no(keys)
        @index_no, index_keys = detect_poor_index_no(keys)
        raise UndefinedIndex.new("Undefined index for keys #{keys}") unless @index_no
        keys = index_keys
      end

      @tuples = case params
      when Hash
        # name: ['a', 'b'], email: ['c', 'd'] => [['a', 'c'], ['b', 'd']]
        values = params.values_at(*keys)
        if values.first.is_a?(Array)
          values.transpose.map{|vals| @record.keys_values_to_tuple(keys, vals)}
        else
          [@record.keys_values_to_tuple(keys, values)]
        end
      when Array
        params.map{|v| @record.keys_values_to_tuple(keys, v.values_at(*keys))}
      end
      self
    end

    # # works fine only on TREE index
    # def batches(count = 1000, &blk)
    #   raise ArgumentError.new("Only one tuple provided in batch selects") if @tuples.size > 1
      
    # end

    # def _batch_exec
    #   Tarantool.call proc_name: 'box.select_range', args: [space_no.to_s, @index_no.to_s, count.to_s] + @tuples.first.map(&:to_s), return_tuple: true
    # end

    def batches_each(&blk)
      batches { |records| records.each(&blk) }
    end

    def all
      to_a
    end

    def first
      limit(1).all.first
    end

    def detect_index_no(keys)
      i = 0
      @record.indexes.each do |index_fields|
        return i if index_fields[0, keys.size] == keys
        i += 1
      end
      nil
    end

    def detect_poor_index_no(keys)
      i = 0
      @record.indexes.each do |index_fields|
        fields = index_fields[0, keys.size]
        return [i, fields] if (fields - keys).empty?
        i += 1
      end
      nil
    end

    def to_records(tuples)
      tuples.map do |tuple|
        @record.from_server(tuple)
      end
    end
  end
  class Record
    extend ActiveModel::Naming
    include ActiveModel::AttributeMethods
    include ActiveModel::Validations
    include ActiveModel::Serialization
    extend ActiveModel::Callbacks
    include ActiveModel::Dirty

    include ActiveModel::Serializers::JSON
    include ActiveModel::Serializers::Xml

    define_model_callbacks :save, :create, :update, :destroy
    define_model_callbacks :initialize, :only => :after

    class_attribute :fields, :field_keys
    self.fields = {}
    self.field_keys = [].freeze

    class_attribute :default_values    
    self.default_values = {}

    class_attribute :primary_index    
    class_attribute :indexes
    self.indexes = []

    class_attribute :space_no
    class_attribute :tarantool
    class << self
      def set_space_no(val)
        self.space_no = val
      end

      def set_tarantool(val)
        self.tarantool = val
      end

      def field(name, type, params = {})
        define_attribute_method name
        self.fields = fields.merge name => { type: type, field_no: fields.size, params: params }
        self.field_keys = self.fields.keys.freeze
        unless self.primary_index
          index name, primary: true
        end
        if params[:default]
          self.default_values = default_values.merge name => params[:default]
        end
        define_method name do
          attributes[name]
        end
        define_method "#{name}=" do |v|
          send("#{name}_will_change!") unless v == attributes[name]
          attributes[name] = v
        end
      end

      def index(*fields)
        options = {}
        options = fields.pop if Hash === fields.last
        if options[:primary]
          self.indexes[0] = fields
          self.primary_index = fields
        else
          self.indexes += [fields]
        end
      end

      def find(*keys)
        res = space.select(*keys)
        if keys.size == 1
          if res.tuple
            from_server res.tuple
          else
            nil
          end
        else
          res.tuples.map { |tuple| from_server tuple }
        end
      end

      def select
        Select.new(self)
      end

      %w{where limit offset call first}.each do |v|
        define_method v do |*args|
          select.send(v, *args)
        end
      end

      def create(attribites = {})
        new(attribites).tap { |o| o.save }
      end

      def from_server(tuple)
        h = tuple_to_hash(tuple)
        h[:__new_record] = false
        new(h)
      end

      def space
        @space ||= tarantool.space(space_no)
      end

      def tuple_to_hash(tuple)
        memo = {}; keys = field_keys
        i = 0; n = keys.size
        while i < n
          unless (v = tuple[i]).nil?
            k = keys[i]
            memo[k] = _cast_tuple_to_value(k, v)
          end
          i += 1
        end
        memo
      end

      def hash_to_tuple(hash)
        field_keys.map{|k| _cast_value_to_tuple(k, hash[k])}
      end

      def keys_values_to_tuple(keys, values)
        i = 0
        values.take_while{|v| !v.nil?}.map do |v|
          k = keys[i]
          i += 1
          _cast_value_to_tuple(k, v)
        end
      end

      ZERO = "\0".freeze
      def _cast_tuple_to_value(name, value)
        serializer = _get_serializer(fields[name][:type])
        raise TarantoolError.new("Value is not a Field") unless value.is_a?(Field)
        return nil if value.data == ZERO
        serializer.decode(value)
      end

      def _cast_value_to_tuple(name, value)
        serializer = _get_serializer(fields[name][:type])
        return ZERO if value.nil?
        serializer.encode(value)
      end

      def _get_serializer(type)
        Serializers::MAP[type] || raise(TarantoolError.new("Undefind serializer #{type}"))
      end
    end

    attr_accessor :__new_record
    def initialize(attributes = {})
      run_callbacks(:initialize) do
        init attributes
      end
    end

    def init(attributes)
      @__new_record = attributes.delete(:__new_record)
      @__new_record = true if @__new_record.nil?
      attributes.each do |k, v|
        send("#{k}=", v)
      end      
    end

    def id
      primary = self.class.primary_index
      if primary.size == 1
        attributes[primary[0]]
      else
        primary.map{ |p| attributes[p] }
      end
    end

    def space
      self.class.space
    end

    def new_record?
      @__new_record
    end

    def attributes
      @attributes ||= self.class.default_values.dup
    end

    def new_record!
      @__new_record = true
    end

    def old_record!
      @__new_record = false
    end

    def in_callbacks(&blk)
      run_callbacks(:save) { run_callbacks(new_record? ? :create : :update, &blk)}
    end

    def save
      in_callbacks do
        if valid?
          if new_record?
            space.insert(*to_tuple)
          else
            return true if changed.size == 0
            ops = []
            changed.each do |k|
              k = k.to_sym
              v = attributes[k]
              ops << [field_no(k), :set, self.class._cast_value_to_tuple(k, v)] if v
            end
            space.update id, ops: ops
          end
          @previously_changed = changes
          @changed_attributes.clear
          old_record!
          true
        else
          false
        end
      end
    end

    def update_attribute(field, value)
      self.send("#{field}=", value)
      save
    end

    def update_attributes(attributes)
      attributes.each do |k, v|
        self.send("#{k}=", v)
      end
      save
    end

    def increment(field, by = 1)
      space.update id, ops: [[field_no(field), :add, by]]
    end

    def destroy
      run_callbacks :destroy do
        space.delete id
        true
      end
    end

    def to_tuple
      self.class.hash_to_tuple attributes
    end

    def field_no(name)
      self.class.fields[name][:field_no]
    end

    def reload
      tuple = space.select(id).tuple
      return false unless tuple
      init self.class.tuple_to_hash(tuple).merge __new_record: false
      self
    end

    def ==(other)
      self.id == other.id
    end

  end
end
