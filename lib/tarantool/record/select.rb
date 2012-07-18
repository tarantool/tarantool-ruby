module Tarantool
  class BaseRecord
    class Select
      include Enumerable

      attr_reader :record, :params
      def initialize(record, params={})
        @record = record
        @params = params
      end

      def space_no
        @record.space_no
      end

      def results
        @results ||= begin
            raise "Condition is not set"  unless @params[:where]
            @record.auto_space.select(
              @params[:where],
              @params[:offset] || 0,
              @params[:limit] || -1
            )
          end
      end

      def reset!
        @results = nil
        self
      end

      def each
        return to_enum  unless block_given?
        results.each{|a| yield a}
      end

      def call(*args)
        @record.call(*args)
      end

      def limit(limit)
        self.class.new(@record, @params.merge(limit: limit))
      end

      def offset(offset)
        self.class.new(@record, @params.merge(offset: offset))
      end

      def where(params)
        self.class.new(@record, @params.merge(where: params))
      end

      def shard(params)
        self.class.new(@record, @params.merge(shard: params))
      end

      def auto_shard
        params = @params.dup
        params.delte :shard
        self.class.new(@record, params)
      end

      def all
        results.dup
      end

      def first
        space.select(@params[:where], @params[:offset] || 0, 1).first
      end

      def space
        space = @record.auto_space
        @params[:shard] ? space.shard(@params[:shard]) : space
      end
    end
  end
end
