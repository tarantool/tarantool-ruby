require 'tarantool'
require 'tarantool/record/select'
require 'active_support/core_ext/class/attribute'

module Tarantool
  class BaseRecord
    class RecordError < StandardError; end
    class UpdateNewRecord < RecordError; end

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
    end

    module ClassMethods
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

        self.fields = fields.merge(name => type)
        index name  if indexes.empty?

        if params[:default]
          self.default_values = default_values.merge name => params[:default]
        end

        define_field_accessor(name, type)
      end

      def index(*fields)
        options = Hash === fields.last ? fields.pop : {}
        if options[:primary]
          self.indexes = indexes.dup.tap{|ind| ind[0] = fields}
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

      %w{where limit offset}.each do |meth|
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
          false
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
        @attributes = space.update(id, ops, return_tuple: true)
        self
      end

      def increment(field, by = 1)
        raise UpdateNewRecord, "Could not call update on new record"  if @__new_record
        update([[field.to_sym, :+, by]])
      end
    end
    include InstanceMethods

  end
end
