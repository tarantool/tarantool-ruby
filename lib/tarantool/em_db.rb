module Tarantool
  class EMDB < DB
    IPROTO_CONNECTION_TYPE = :em_callback

    class OneShardRead
      def initialize(replicas, i, request_type, body, cb)
        @replicas = replicas
        @i = i
        @request_type = request_type
        @body = body
        @cb = cb
      end

      def call(result)
        case result
        when ::IProto::ConnectionError
          if (@i += 1) == @replicas.size
            @cb.call(ConnectionError.new("no available connections"))
          end
          @replicas[@i].send_request(@request_type, @body, self)
        else
          @cb.call(result)
        end
      end
    end

    def _one_shard_read(replicas, request_type, body, cb)
      replicas[0].send_request(request_type, body, 
                               OneShardRead.new(replicas, 0, request_type, body, cb))
    end

    class OneShardWrite
      def initialize(replicas, i, request_type, body, cb)
        @replicas = replicas
        @i = i
        @request_type = request_type
        @body = body
        @cb = cb
      end

      def call(result)
        case result
        when ::IProto::ConnectionError, ::Tarantool::NonMaster
          @replicas.rotate!
          if (@i -= 1) == 0
            @cb.call(NoMasterError.new("no available master connections"))
          end
          @replicas[@i].send_request(@request_type, @body, self)
        else
          @cb.call(result)
        end
      end
    end

    def _one_shard_write(replicas, request_type, body, cb)
      replicas[0].send_request(request_type, body,
                               OneShardWrite.new(replicas, replicas.size, request_type, body, cb))
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
