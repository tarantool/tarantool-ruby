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
        shards = [ [ _fix_connection(conn) ] ]
      else
        shards = conf[:servers]
        unless shards.is_a? Array
          shards = [ shards ]
        end
        unless shards.first.is_a? Array
          shards = [ shards ]
        end
        shards = shards.map{|shard| shard.map{|server| _fix_connection(conn)}}
      end

      shard_strategy = conf[:shard_strategy] || :round_robin
      if %w{round_robin master_first}.include?(shard_strategy)
        shard_strategy = shard_strategy.to_sym
      end
      unless [:round_robin, :master_first].include?(shard_strategy)
        raise ArgumentError, "Shard strategy could be :round_robin or :master_first, got #{shard_strategy.inspect}"
      end

      case conf[:type] || :block
      when :em, :em_fiber
        require 'tarantool/fiber_db'
        FiberDB.new(shards, shard_strategy)
      when :em_cb, :em_callback
        require 'tarantool/callback_db'
        CallbackDB.new(shards, shard_strategy)
      when :block
        require 'tarantool/block_db'
        BlockDB.new(shards, shard_strategy)
      end
    end

    private
    def _fix_connection(conn)
      if conn.is_a? Hash
        conn = [conf[:host], conf[:port]].compact.join(':')
      end
      if conn.is_a? String
        host, port = conn.split(':')
        conn = [host, port || DEFAULT_PORT]
      end
      raise ArgumentError, "Wrong connection declaration #{conn}" unless conn.is_a? Array
      conn
    end
  end

  class DB
    attr_reader :closed, :connection
    alias closed? closed
    def initialize(shards, shard_strategy)
      @shards = shards
      @shard_strategy = shard_strategy
      @connections = {}
      @closed = false
    end

    # returns regular space, where fields are named by position
    #
    # tarantool.space_block(0, :int, :str, :int, :str, indexes: [[0], [1,2]])
    def space_array(space_no, *args)
      options = args.pop  if Hash === args.last
      options ||= {}
      fields = args
      fields.flatten!
      primary_key = options[:pk]
      indexes = options[:indexes]
      self.class::SpaceArray.new(self, space_no, fields, primary_key, indexes)
    end
    # alias space_array to space for backward compatibility
    alias space space_array

    def space_hash(space_no, fields, opts = {})
      primary_key = opts[:pk]
      indexes = opts[:indexes]
      self.class::SpaceHash.new(self, space_no, fields, primary_key, indexes)
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

    def _send_request(shard_numbers, read_write, request_type, body, cb)
      if @closed
        cb.call(::IProto::Disconnected.new("Tarantool is closed"))
      else
        if Array === shard_numbers
          if shard_numbers.size > 1
            return _send_to_several_shards(
                      shard_numbers, read_write, request_type, body, cb
                   )
          else
            shard_numbers = shard_numbers[0]
          end
        end
        _send_to_one_shard(shard_numbers, read_write, request_type, body, cb)
      end
    end

    def _send_to_one_shard(shard_number, read_write, request_type, body, cb)
      if (replicas = _shard(shard_number)).size == 1
        replicas[0].send_request(request_type, body, cb)
      elsif read_write == :read
        replicas = replicas.shuffle  if @shard_strategy == :round_robin
        _one_shard_read(replicas, request_type, body, cb)
      else
        _one_shard_write(replicas, request_type, body, cb)
      end
    end

  end
end
