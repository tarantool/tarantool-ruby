module Tarantool
  class Record
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
            to_records @record.space.select(
              @params[:where],
              @params[:offset] || 0,
              @params[:limit] || -1
            )
          end
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

      def all
        results.dup
      end

      def first
        limit(1).results.first
      end

      def to_records(hashes)
        hashes.map{ |hash| @record.from_server(hash) }
      end
    end
  end
end
