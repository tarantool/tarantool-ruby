require 'minitest/spec'
require 'minitest/autorun'

require 'tarantool'

TCONFIG = { host: '127.0.0.1', port: 33013, admin: 33015 }

SPACE0 = {
  types:  [:str, :str, :str, :int],
  pk:     [0],
  indexes:[[1,2], [3]]
}
SPACE1 = {
  types:  [:int, :str, :int, 2],
  pk:     [0],
  indexes:[]
}
SPACE2 = {
  types:  [:str, :str, :int],
  pk:     [0,1],
  indexes:[[2]]
}

HSPACE0 = {
  fields: {name: :str, surname: :str, email: :str, score: :int},
  pk: :name,
  indexes: [%w{surname email}, 'score']
}
HSPACE1 = {
  fields: {id: :int, _tail: [:str, :int]},
  pk: [:id],
  indexes: nil
}
HSPACE2 = {
  fields: {first: :str, second: :str, third: :int},
  pk: %w{first second},
  indexes: :third
}

module Helper
  def exec_tarantool(cmd)
    cnf = TCONFIG
    tarant = %W{tarantool -p #{cnf[:port]} -m #{cnf[:admin]}}
    tarant = [{}, *tarant, :err => [:child, :out]]
    cmd = cmd.gsub(/^\s+/, '')
    #puts cmd
    ret = IO.popen(tarant, 'w+'){|io|
      io.write(cmd)
      io.close_write
      io.read
    }
    #puts "tarantool printed #{ret}"
    assert $?.success?, "Tarantool command line failed: #{ret}"
  end

  def truncate
    exec_tarantool "
       lua truncate(0)
       lua truncate(1)
       lua truncate(2)
    "
  end

  def seed
    exec_tarantool <<-EOF
      insert into t0 values ('vasya', 'petrov', 'eb@lo.com', 5)
      insert into t0 values ('ilya', 'zimov', 'il@zi.bot', 13)
      insert into t0 values ('fedor', 'kuklin', 'ku@kl.in', 13)
      insert into t1 values (1, 'common', 4)
      insert into t1 values (2, 'medium', 6, 'common', 7)
      insert into t2 values ('hi zo', 'ho zo', 1)
      insert into t2 values ('hi zo', 'pidas', 1, 3, 5)
      insert into t2 values ('coma', 'peredoma', 2)
    EOF
  end

  def clear_db
    truncate
    seed
  end

  def emrun(semaphore = 1)
    @semaphore = semaphore
    EM.run {
      @timeout_timer = EM.add_timer(1) {
        EM.stop
        assert false, "timeout encounted"
      }
      yield
    }
  end

  def emstop
    @semaphore -= 1
    if @semaphore == 0
      EM.cancel_timer @timeout_timer
      EM.next_tick{ EM.stop }
    end
  end

  def fibrun
    res = nil
    EM.run {
      f = Fiber.new{
        begin
          res = yield
        ensure
          EM.next_tick{ EM.stop }
        end
      }
      EM.next_tick{ f.resume }
    }
    res
  end
end

class MiniTest::Unit::TestCase
  include ::Helper
end
