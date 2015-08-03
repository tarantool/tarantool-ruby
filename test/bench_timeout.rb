require 'tarantool16'
require 'timeout'

def bench_1000_get(kind, timeout=nil)
  case kind
  when :no, :external
    db = Tarantool16.new host: '127.0.0.1:33013'
  when :native
    db = Tarantool16.new host: '127.0.0.1:33013', timeout: timeout
  end
  now = Time.now.to_f
  if kind == :external
    10000.times do
      Timeout.timeout(1) do
        db.get(:test, 1)
      end
    end
  else
    10000.times do
      db.get(:test, 1)
    end
  end
  r = Time.now.to_f - now
  db.conn.disconnect
  r
end

puts "Without timeout: %f" % bench_1000_get(:no, nil)
puts "With timeout: %f" % bench_1000_get(:native, 1)
puts "With Timeout: %f" % bench_1000_get(:external, 1)
puts "Without timeout: %f" % bench_1000_get(:no, nil)
puts "With timeout: %f" % bench_1000_get(:native, 1)
puts "With Timeout: %f" % bench_1000_get(:external, 1)
puts "Without timeout: %f" % bench_1000_get(:no, nil)
puts "With timeout: %f" % bench_1000_get(:native, 1)
puts "With Timeout: %f" % bench_1000_get(:external, 1)
