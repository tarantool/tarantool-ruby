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
        @new_record = false
      else
        self.class.replace(@attributes)
      end
      self
    end

    # update record in db first, reload it then
    #
    #   record.update({:state => 'sleep', :sleep_count => [:+, 1]})
    #   record.update([[:state, 'sleep'], [:sleep_count, :+, 1]])
    def update(ops)
      raise UpdateNewRecord, "Could not call update on new record"  if @new_record
      @attributes = space.update(id, ops, return_tuple: true)
      self
    end

    def increment(field, by = 1)
      raise UpdateNewRecord, "Could not call update on new record"  if @new_record
      update([[field.to_sym, :+, by]])
    end

    def destroy
      self.class.delete id
    end

    def reload
      if hash = space.by_pk(id)
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

    class_attribute :fields, instance_reader: false, instance_writer: false
    self.fields = {}.freeze

    class_attribute :default_values, instance_reader: false, instance_writer: false
    self.default_values = {}.freeze

    class_attribute :indexes, instance_reader: false, instance_writer: false
    self.indexes = [].freeze

    class_attribute :space_no, instance_reader: false, instance_writer: false
    class_attribute :tarantool, instance_reader: false, instance_writer: false

    class << self
      alias set_space_no space_no=
      alias set_tarantool tarantool=

      def generated_attribute_methods
        @generated_attribute_methods ||= begin
            include (mod = Module.new)
            mod
          end
      end

      def field(name, type, params = {})
        unless Symbol === type
          if type == Integer
            type = :integer
          elsif type == String
            type = :string
          elsif type.respond_to?(:encode) && type.respond_to?(:decode)
            # then all good
          elsif sr = Serializers::MAP.rassoc(type)
            type = sr[0]
          else
            raise "Unknown serializer #{type}"
          end
        end

        self.fields = fields.merge(name => type).freeze
        index name  if indexes.empty?

        if params[:default]
          self.default_values = default_values.merge(name=>params[:default]).freeze
        end

        generated_attribute_methods.class_eval <<-"EOF", __FILE__, __LINE__ - 1
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
            tarantool.space_hash(space_no, fields.dup, pk: pk, indexes: indexes)
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

      def create(attrs)
        new(attrs).save
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
      alias all select

      def insert(hash, ret_tuple = false)
        if ret_tuple
          from_fetched space.insert(hash, return_tuple: true)
        else
          space.insert(hash)
        end
      end

      def replace(hash, ret_tuple = false)
        if ret_tuple
          from_fetched space.replace(hash, return_tuple: true)
        else
          space.replace(hash)
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

      def invoke(proc_name, *args)
        opts = Hash === args.last ? args.pop : {}
        space.invoke(proc_name, args, opts)
      end

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
    end
  end
end
