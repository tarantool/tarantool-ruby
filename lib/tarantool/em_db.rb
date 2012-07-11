module Tarantool
  class EMDB < DB
    IPROTO_CONNECTION_TYPE = :em_callback
    INITIAL = Object.new.freeze

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

    class OneShardRead
      def initialize(replicas, request_type, body, cb)
        @replicas = replicas
        @i = -1
        @request_type = request_type
        @body = body
        @cb = cb
      end

      def call(result)
        case result
        when INITIAL, ::IProto::ConnectionError
          begin
            if (@i += 1) >= @replicas.size
              return @cb.call(ConnectionError.new("no available connections"))
            end
          end until (repl = @replicas[@i]).could_be_connected?
          repl.send_request(@request_type, @body, self)
        else
          @cb.call(result)
        end
      end
    end

    def _one_shard_read(replicas, request_type, body, cb)
      OneShardRead.new(replicas, request_type, body, cb).call(INITIAL)
    end

    class OneShardWrite
      def initialize(replicas, request_type, body, cb)
        @replicas = replicas
        @i = replicas.size
        @request_type = request_type
        @body = body
        @cb = cb
      end

      def rotate!
        if (@i -= 1) <= 0
          return @cb.call(NoMasterError.new("no available master connections"))
        end
        @replicas.rotate!
      end

      def call(result)
        case result
        when INITIAL, ::IProto::ConnectionError, ::Tarantool::NonMaster
          rotate!  if Exception === result
          rotate!  until (repl = @replicas[0]).could_be_connected?
          repl.send_request(@request_type, @body, self)
        else
          @cb.call(result)
        end
      end
    end

    def _one_shard_write(replicas, request_type, body, cb)
       OneShardWrite.new(replicas, replicas.size, request_type, body, cb).call(INITIAL)
    end

    class Concatter
      def initialize(result, count, cb)
        @result = result
        @count = count
        @cb = cb
      end
      def call(array)
        if @count > 0
          case array
          when Array
            @result.concat array
          when Exception
            @result = array
            @count = 1
          else
            @result << array
          end
          if (@count -= 1) == 0
            @cb.call @result
          end
        end
      end
    end

    def _send_to_several_shards(shard_numbers, read_write, request_type, body, cb)
      concatter = Concatter.new([], shard_numbers.size, cb)
      for shard in shard_numbers
        _send_to_one_shard(shard, read_write, request_type, body, concatter)
      end
    end
  end
end
