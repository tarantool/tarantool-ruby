require 'eventmachine'
require "iproto"
require "tarantool/version"
require "tarantool/exceptions"
require "tarantool/request"
require "tarantool/response"
require "tarantool/space_array.rb"
require "tarantool/space_hash.rb"
require "tarantool/query.rb"
require "tarantool/serializers.rb"

module Tarantool
  #autoload :Record, 'tarantool/record'
  #autoload :LightRecord, 'tarantool/light_record'
  DEFAULT_PORT = 33013

  class << self
    def new(conf)
      if conf[:host]
        shards = [ [ _fix_connection(conf) ] ]
      else
        shards = conf[:servers]
        unless shards.is_a? Array
          shards = [ shards ]
        end
        unless shards.first.is_a? Array
          shards = [ shards ]
        end
        shards = shards.map{|shard| shard.map{|server| _fix_connection(server)}}
      end

      replica_strategy = conf[:replica_strategy] || :round_robin
      if %w{round_robin master_first}.include?(replica_strategy)
        replica_strategy = replica_strategy.to_sym
      end
      unless [:round_robin, :master_first].include?(replica_strategy)
        raise ArgumentError, "Shard strategy could be :round_robin or :master_first, got #{replica_strategy.inspect}"
      end

      case conf[:type] || :block
      when :em, :em_fiber
        require 'tarantool/fiber_db'
        FiberDB.new(shards, replica_strategy)
      when :em_cb, :em_callback
        require 'tarantool/callback_db'
        CallbackDB.new(shards, replica_strategy)
      when :block
        require 'tarantool/block_db'
        BlockDB.new(shards, replica_strategy)
      else
        raise "Unknown Tarantool connection type"
      end
    end

    private
    def _fix_connection(conn)
      if conn.is_a? Hash
        conn = [conn[:host], conn[:port]].compact.join(':')
      end
      if conn.is_a? String
        host, port = conn.split(':')
        port ||= DEFAULT_PORT
        conn = [host, port.to_i]
      end
      raise ArgumentError, "Wrong connection declaration #{conn}" unless conn.is_a? Array
      conn
    end
  end

  class DB
    attr_reader :closed, :connection
    alias closed? closed
    def initialize(shards, replica_strategy)
      @shards = shards
      @replica_strategy = replica_strategy
      @connections = {}
      @closed = false
    end

    # returns regular space, where fields are named by position
    #
    # tarantool.space_block(0, [:int, :str, :int, :str], keys: [[0], [1,2]])
    def space_array(space_no, field_types = [], opts = {})
      indexes = opts[:keys] || opts[:indexes]
      shard_fields = opts[:shard_fields]
      shard_proc = opts[:shard_proc]
      self.class::SpaceArray.new(self, space_no, field_types, indexes,
                                 shard_fields, shard_proc)
    end
    # alias space_array to space for backward compatibility
    alias space space_array

    def space_hash(space_no, fields, opts = {})
      indexes = opts[:keys] || opts[:indexes]
      shard_fields = opts[:shard_fields]
      shard_proc = opts[:shard_proc]
      self.class::SpaceHash.new(self, space_no, fields, indexes,
                                shard_fields, shard_proc)
    end

    def query
      @query ||= self.class::Query.new(self)
    end

    def method_missing(name, *args)
      if query.respond_to?(name)
        query.send(name, *args)
      else
        super
      end
    end

    def close
      @closed = true
      close_connection
    end

    def shards_count
      @shards.count
    end

    def _shard(number)
      @connections[number] ||= begin
        @shards[number].map do |host, port|
          IProto.get_connection(host, port, self.class::IPROTO_CONNECTION_TYPE)
        end
      end
    end

    def close_connection
      @connections.each do |name, conn|
        conn.close
      end
      @connections.clear
    end
  end
end
