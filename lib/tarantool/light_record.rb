require 'tarantool'
require 'active_support/core_ext/class/attribute'

module Tarantool
  class LightRecord
    class LightRecordError < StandardError; end
    class UpdateNewRecord < LightRecordError; end

    attr :new_record, :attributes
    alias new_record? new_record
    def initialize(hash = nil)
      @new_record = true
      @attributes = self.class.default_values.dup
      set_attributes(hash)  if hash
      after_init
    end

    def _tail
      @attributes[:_tail]
    end

    def _tail=(v)
      @attributes[:_tail] = v
    end

    def set_attributes(hash)
      for k, v in hash
        send("#{k}=", v)
      end
    end

    def __fetched(hash)
      @new_record = false
      @attributes = hash
      after_init
      self
    end

    # callback which runs both after initialization and after
    # fetching from database
    def after_init
    end

    def update_attributes(hash)
      set_attributes(hash)
      save
    end

    def pk_attributes
      @pk_attributes ||= self.class.primary_index
    end

    def id
      (primary = pk_attributes).size == 1 ?
        @attributes[primary[0]] :
        @attributes.values_at(*primary)
    end

    def new_record!
      @new_record = true
      self
    end

    def old_record!
      @new_record = false
      self
    end

    def space
      self.class.space
    end

    def save
      if @new_record
        self.class.insert(@attributes)
      else
        self.class.replace(@attributes)
      end
      self
    end

    def update(ops)
      raise UpdateNewRecord, "Could not call update on new record"  if @new_record
      @arguments = self.class.update(name, ops, true)
      self
    end

    def destroy
      self.class.delete id
    end

    def reload
      if hash = self.class.by_pk(id)
        @new_record = false
        @attributes = hash
        self
      else
        false
      end
    end

    def ==(other)
      other.class == self.class && id == other.id
    end

    class FieldDef < Struct.new(:type, :field_no, :param); end

    class << self
      class_attribute :field_types
      self.field_types = {}.freeze

      class_attribute :default_values
      self.default_values = {}.freeze

      class_attribute :indexes
      self.indexes = [].freeze

      class_attribute :space_no
      class_attribute :tarantool
      
      alias set_space_no space_no=
      alias set_tarantool tarantool=

      def generated_attribute_methods
        @generated_attribute_methods ||= begin
            include (mod = Module.new)
            mod
          end
      end

      def field(name, type, params = {})
        if Class === type
          if type == Integer
            type = :integer
          elsif type == String
            type = :string
          elsif sr = Serializers::MAP.rassoc(type)
            type = sr[0]
          else
            raise "Unknown serializer #{type}"
          end
        end

        self.field_types = field_types.merge(name => type).freeze
        index name  if indexes.empty?

        if params[:default]
          self.defaults_values = default_values.merge(name=>params[:default]).freeze
        end

        generated_attribute_methods.class_eval <<-"EOF", __FILE__, __LINE__
          def #{name}
            @attributes[:"#{name}"]
          end

          def #{name}=(v)
            @attributes[:"#{name}"] = v
          end
        EOF
      end

      def index(*fields)
        options = Hash === fields.last ? fields.pop : {}
        if options[:primary]
          self.indexes = indexes.dup.tup{|ind| ind[0] = fields}
        else
          self.indexes += [fields]
        end
      end

      def primary_index
        indexes[0]
      end

      def space
        @space ||= begin
            pk, *indexes = indexes()
            tarantool.space_hash(space_no, field_types, pk: pk, indexes: indexes)
          end
      end

      # space that will return records as results for calls
      # it is useful, if you wish to use callback interface
      def auto_space
        @auto_space ||= begin
            space.with_translator(method(:from_fetched))
          end
      end

      def by_pk(pk)
        if Hash === (res = space.by_pk(pk))
          from_fecthed(res)
        end
      end

      def by_pks(pks)
        space.all_by_pks(pks).map{|hash| from_fetched(hash)}
      end

      def create(attrs)
        insert(attrs, true)
      end

      def first(cond)
        if Hash === (res = space.first(cond))
          from_fetched(res)
        end
      end

      def select(cond, opts={})
        res = space.all(cond, opts)
        res.map!{|hash| from_fetched(hash)}
        res
      end

      def insert(hash, ret_tuple = false)
        if ret_tuple
          from_fetched space.insert(hash, return_tuple: true)
        else
          space.insert(hash)
        end
      end

      def replace(hash, ret_tuple = false)
        if ret_tuple
          from_fetched space.insert(hash, return_tuple: true)
        else
          space.insert(hash)
        end
      end

      def update(pk, ops, ret_tuple=false)
        if ret_tuple
          from_fetched space.update(pk, ops, return_tuple: true)
        else
          space.update(pk, ops)
        end
      end

      def invoke(proc_name, *args)
        opts = Hash === args.last ? args.pop : {}
        space.call(proc_name, args, opts)
      end

      def call(proc_name, *args)
        opts = Hash === args.last ? args.pop : {}
        res = space.call(proc_name, args, opts)
        if Array === res && !opts[:returns]
          res.map{|hash| from_server(hash) }
        else
          res
        end
      end

      def from_fetched(hash)
        allocate.__fetched(hash)
      end
    end
  end
end
