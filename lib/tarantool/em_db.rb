module Tarantool
  class EMDB < DB
    IPROTO_CONNECTION_TYPE = :em_callback
    INITIAL = Object.new.freeze

    class Curry1 < Struct.new(:obj, :arg)
      def call
        obj.call arg
      end
    end

    class FeedResponse < Struct.new(:response)
      def call(result)
        if Exception === result
          response.cb.call result
        else
          response.call_callback(result)
        end
      end
    end

    def _send_request(shard_numbers, read_write, request_type, body, response)
      if @closed
        exc =  ::IProto::Disconnected.new("Tarantool is closed")
        if EM.reactor_running?
          EM.next_tick Curry1.new(response.cb, exc)
        else
          response.cb.call exc
        end
      else
        feed = FeedResponse.new(response)
        shard_numbers = shard_numbers[0]  if Array === shard_numbers && shard_numbers.size == 1
        if Array === shard_numbers
          _send_to_several_shards(shard_numbers, read_write, request_type,
                                  body, response, feed)
        else
          _send_to_one_shard(shard_numbers, read_write, request_type,
                             body, response, feed)
        end
      end
    end

    def _send_to_one_shard(shard_number, read_write, request_type, body, response, feed)
      if (replicas = _shard(shard_number)).size == 1
        replicas[0].send_request(request_type, body, response)
      elsif read_write == :read
        replicas = replicas.shuffle  if @replica_strategy == :round_robin
        EM.next_tick OneShardRead.new(replicas, request_type, body, response, feed)
      else
        EM.next_tick OneShardWrite.new(replicas, request_type, body, response, feed)
      end
    end

    class OneShardRead
      include ParseIProto
      def initialize(replicas, request_type, body, response, feed)
        @replicas = replicas
        @i = -1
        @request_type = request_type
        @body = body
        @response = response
        @feed = feed
      end

      def call(result=INITIAL)
        result = _parse_iproto(result)  unless result == INITIAL
        case result
        when INITIAL, ::IProto::ConnectionError
          begin
            if (@i += 1) >= @replicas.size
              EM.next_tick Curry1.new(@feed, ConnectionError.new("no available connections"))
              return
            end
          end until (repl = @replicas[@i]).could_be_connected?
          repl.send_request(@request_type, @body, self)
        when Exception
          @feed.call result
        else
          @feed.call @response.parse_response(result)
        end
      end
    end

    class OneShardWrite
      include ParseIProto
      def initialize(replicas, request_type, body, response, feed)
        @replicas_origin = replicas
        @replicas = replicas.dup
        @i = replicas.size
        @request_type = request_type
        @body = body
        @response = response
        @feed = feed
      end

      def rotate!
        if @i > 0
          @i -= 1
          @replicas.rotate!
        end
      end

      def call(result=INITIAL)
        result = _parse_iproto(result)  unless result == INITIAL
        case result
        when INITIAL, ::IProto::ConnectionError, ::Tarantool::NonMaster
          rotate!  if Exception === result
          rotate!  until @i <= 0 || (repl = @replicas[0]).could_be_connected?
          if @i <= 0
            EM.next_tick Curry1.new(@feed, NoMasterError.new("no available master connections"))
            return
          end
          repl.send_request(@request_type, @body, self)
        when Exception
          @feed.call result
        else
          @replicas_origin.replace @replicas
          @feed.call @response.parse_response(result)
        end
      end
    end

    class Concatter
      def initialize(count, feed)
        @result = []
        @count = count
        @feed = feed
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
            if Array === @result && Integer === @result.first
              @feed.call @result.inject(0){|s, i| s + i}
            else
              @feed.call @result
            end
          end
        end
      end
    end

    class ConcatterReplace
      def initialize(count, feed)
        @result = []
        @count = count
        @feed = feed
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
            if Array === @result && Integer === @result.first
              @feed.call @result.inject(0){|s, i| s + i}
            else
              @feed.call @result
            end
          end
        end
      end
    end

    class ConcatterReplace
      def initialize(count, feed)
        @result = []
        @count = count
        @feed = feed
      end
      def call(array)
        if @count > 0
          case array
          when Array
            @result.concat array
          when ::Tarantool::TupleDoesntExists
            @result << array
          when Exception
            @result = array
            @count = 1
          else
            @result << array
          end
          if (@count -= 1) == 0
            if Exception === @result
              @feed.call @result
            elsif @result.all?{|r| ::Tarantool::TupleDoesntExists === r}
              @feed.call @result.first
            else
              @result.delete_if{|r| ::Tarantool::TupleDoesntExists === r}
              if Integer === @result.first
                @feed.call @result.inject(0){|s, i| s + i}
              else
                @feed.call @result
              end
            end
          end
        end
      end
    end

    def _send_to_several_shards(shard_numbers, read_write, request_type, body, response, feed)
      concat = read_write != :replace ? Concatter.new(shard_numbers.size, feed) :
                                        ConcatterReplace.new(shard_numbers.size, feed)
      for shard in shard_numbers
        _send_to_one_shard(shard, read_write, request_type, body, response, concat)
      end
    end
  end
end
