require 'active_model'
require 'tarantool'
require 'tarantool/record/select'

module Tarantool
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
        case val.class.name
        when 'Tarantool::BlockDB', 'Tarantool::FiberDB'
          self.tarantool = val
        else
          raise "Tarantool should be blocking of fibered!!! (i.e. of class Tarantool::BlockDB or Tarantool::FiberDB) (got #{val.class})"
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
        self.fields = fields.merge name => { type: type, field_no: fields.size, params: params }
        self.field_keys = self.fields.keys.freeze

        unless self.primary_index
          index name, primary: true
        end

        if params[:default]
          self.default_values = default_values.merge name => params[:default]
        end

        convert_code = case type
           when :int, :integer
             "v = v.to_i  if String === v"
           when :str, :string
             ""
           else
             if serializer = Serializers::MAP[type]
               "v = Serializers::MAP[#{type.inspect}].decode(v)  if String === v"
             else
               raise ValueError, "unknown field type #{type.inspect}"
             end
           end
        define_attribute_method name
        generated_attribute_methods.class_eval <<-"EOF"
          def #{name}
            @attributes[:"#{name}"]
          end
        
          def #{name}=(v)
            #{convert_code}
            #{name}_will_change!  unless v == @attributes[:"#{name}"] || new_record?
            @attributes[:"#{name}"] = v
          end
        EOF
      end

      def index(*fields)
        options = {}
        options = fields.pop if Hash === fields.last
        if options[:primary]
          self.indexes = indexes.dup.tap{|ind| ind[0] = fields}
          self.primary_index = fields
        else
          self.indexes += [fields]
        end
      end

      def find(*keys)
        res = space.all_by_pks(keys)
        if keys.size == 1 && res.size <= 1
          unless res.empty?
            from_server res.first
          else
            nil
          end
        else
          res.tuples.map { |hash| from_server hash }
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

      def first(key)
        find(key)
      end

      def invoke(proc_name, *args)
        space.invoke(proc_naem, args)
      end

      def call(proc_name, *args)
        opts = Hash === args.last ? args.pop.dup : {}
        space.call(proc_name, args, opts).map{|hash|
          from_server(hash)
        }
      end

      def create(attributes = {})
        new(attributes).tap { |o| o.save }
      end

      def from_server(hash)
        allocate.init_fetched(hash)
      end

      def space
        @space ||= begin
            fields_def = {}
            fields.each{|name, desc| fields_def[name.to_sym] = desc[:type]}
            pk = primary_index
            indexes = indexes()[1..-1]
            tarantool.space_hash(space_no, fields_def, pk: pk, indexes: indexes)
          end
      end
    end

    attr_accessor :__new_record
    def initialize(attributes = {})
      @__new_record = true
      run_callbacks(:initialize) do
        init attributes
      end
    end

    def init(attributes)
      @attributes = self.class.default_values.dup
      attributes.each do |k, v|
        send("#{k}=", v)
      end      
    end

    def init_fetched(attributes)
      @__new_record = false
      run_callbacks(:initialize) do
        @attributes = attributes
      end
      self
    end

    def id
      primary = self.class.primary_index
      primary.size == 1 ?
        @attributes[primary[0]] : 
        @attributes.values_at(*primary)
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

    def _in_callbacks(&blk)
      run_callbacks(:save) {
        run_callbacks(new_record? ? :create : :update, &blk)
      }
    end

    def save
      _in_callbacks do
        if valid?
          if new_record?
            @attributes = space.insert(@attributes, return_tuple: true)
          else
            return true if changed.size == 0
            ops = {}
            changed.each do |k|
              k = k.to_sym
              ops[k] = attributes[k]
            end
            @attributes = space.update id, ops, return_tuple: true
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
      space.update id, [[field.to_sym, :add, by]]
    end

    def destroy
      run_callbacks :destroy do
        space.delete id
        true
      end
    end

    def reload
      if hash = space.by_pk(id)
        init_fetched space.by_pk(id)
      else
        false
      end
    end

    def ==(other)
      self.id == other.id
    end
  end
end
