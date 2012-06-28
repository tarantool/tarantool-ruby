require 'em-tarantool'

EM.run do
  tar = EM::Tarantool.new('127.0.0.1', 33013)
  sp = tar.space_plain(0, :int, :int, :str, :str, :int, indexes: [[1,2]])
  sp.by_pk(0){|res|
    puts "Result: #{res.inspect}"
    sp.insert([1, 3, 'reqw', 'rewq', 'rewq'], return_tuple: true) {|af|
      puts "Affected #{af.inspect}"
      sp.update(1, {1=>[:+, 2]}, return_tuple: true){|res|
        puts "Updated #{res}"
        sp.all_by_keys(1, [[5, 'reqw'],[100]]){|res|
          puts "Results: #{res.inspect}"
          sp.delete(1, return_tuple: true) {|res|
            puts "Deleted: #{res}"
            sp.all_by_keys(1, []){|res|
              puts "All: #{res}"
              EM.stop
            }
          }
        }
      }
    }
  }
end
