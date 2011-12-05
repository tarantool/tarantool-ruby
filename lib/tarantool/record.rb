require 'active_model'
require 'tarantool/synchrony'
module Tarantool
  class Select
    include Enumerable
    attr_reader :record
    def initialize(record)
      @record = record
    end

    def space_no
      record.space_no
    end

    def each(&blk)
      res = Tarantool.select(*@tuples, index_no: @index_no, limit: @limit, offset: @offset).tuples
      res.each do |tuple|
        blk.call record.from_server(tuple)
      end
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
      keys, @tuples = case params
      when Hash
        ordered_keys = record.ordered_keys params.keys
        # name: ['a', 'b'], email: ['c', 'd'] => [['a', 'c'], ['b', 'd']]
        if params.values.first.is_a?(Array)          
          [ordered_keys, params[ordered_keys.first].zip(*ordered_keys[1, ordered_keys.size].map { |k| params[k] })]
        else
          [ordered_keys, [record.hash_to_tuple(params)]]
        end
      when Array
        [params.first.keys, params.map { |v| record.hash_to_tuple(v) }]
      end
      @index_no = detect_index_no keys
      raise ArgumentError.new("Undefined index for keys #{keys}") unless @index_no
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
      index_no = nil
      record.indexes.each_with_index do |v, i|
        keys_inst = keys.dup
        v.each do |index_part|
          unless keys_inst.delete(index_part)
            break
          end
          if keys_inst.size == 0
            index_no = i
          end
        end
        break if index_no
      end
      index_no
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

    class_attribute :fields
    self.fields = {}

    class_attribute :default_values    
    self.default_values = {}

    class_attribute :primary_index    
    class_attribute :indexes
    self.indexes = []

    class_attribute :space_no
    define_attr_method :space_no do
      original_space_no || 0
    end
    class << self
      def field(name, type, params = {})
        define_attribute_method name
        self.fields = fields.merge name => { type: type, field_no: fields.size, params: params }
        unless self.primary_index
          self.primary_index = name
          index name
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
        self.indexes = (indexes.dup << fields).sort_by { |v| v.size }
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

      %w{where limit offset}.each do |v|
        define_method v do |*args|
          select.send(v, *args)
        end
      end

      def create(attribites = {})
        new(attribites).tap { |o| o.save }
      end

      def from_server(tuple)
        new(tuple_to_hash(tuple)).tap { |v| v.old_record! }
      end

      def space
        @space ||= Tarantool.space space_no
      end

      def tuple_to_hash(tuple)
        fields.keys.zip(tuple).inject({}) do |memo, (k, v)|
          memo[k] = _cast(k, v) unless v.nil?
          memo
        end
      end

      def hash_to_tuple(hash, with_nils = false)
        res = []
        fields.keys.each do |k|
          v = hash[k]
          res << _cast(k, v) if with_nils || !v.nil?
        end
        res
      end

      def ordered_keys(keys)
        fields.keys.inject([]) do |memo, k|
          keys.each do |k2|
            memo << k2 if k2 == k
          end
          memo
        end
      end

      def _cast(name, value)
        type = self.fields[name][:type]
        serializer = _get_serializer(type)
        if value.is_a?(Field)
          return nil if value.data == "\0"
          serializer.decode(value)
        else
          return "\0" if value.nil?
          serializer.encode(value)
        end
      end

      def _get_serializer(type)
        Serializers::MAP[type] || raise(TarantoolError.new("Undefind serializer #{type}"))
      end
    end

    attr_accessor :new_record
    def initialize(attributes = {})
      attributes.each do |k, v|
        send("#{k}=", v)
      end
      @new_record = true
    end

    def id
      attributes[self.class.primary_index]
    end

    def space
      self.class.space
    end

    def new_record?
      @new_record
    end

    def attributes
      @attributes ||= self.class.default_values.dup
    end

    def new_record!
      @new_record = true
    end

    def old_record!
      @new_record = false
    end

    def save
      def in_callbacks(&blk)
        run_callbacks(:save) { run_callbacks(new_record? ? :create : :update, &blk)}
      end
      in_callbacks do
        if valid?
          if new_record?
            space.insert(*to_tuple)
          else
            ops = changed.inject([]) do |memo, k|
              k = k.to_sym
              memo << [field_no(k), :set, self.class._cast(k, attributes[k])] if attributes[k]
              memo
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
      self.class.hash_to_tuple attributes, true
    end

    def field_no(name)
      self.class.fields[name][:field_no]
    end

    # return new object, not reloading itself as AR-model
    def reload
      self.class.find(id)
    end

  end
end
