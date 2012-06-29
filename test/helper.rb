require 'minitest/spec'
require 'minitest/autorun'

require 'em-tarantool'

TCONFIG = { host: '127.0.0.1', port: 33013, admin: 33015 }

DB = EM::Tarantool.new TCONFIG[:host], TCONFIG[:port]
SPACE0 = {
  types:  [:str, :str, :str, :int],
  pk:     [0],
  indexes:[[1,2]]
}
SPACE1 = {
  types:  [:int],
  pk:     [0],
  indexes:[]
}
SPACE2 = {
  types:  [:str, :str, :int],
  pk:     [0,1],
  indexes:[[2]]
}

module Helper
  def set_timer
    EM.add_timer(0.3) {
      EM.stop
      assert false, "timeout encounted"
    }
  end

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
      insert into t1 values (1)
      insert into t1 values (2, 'medium')
      insert into t3 values ('hi zo', 'ho zo', 1)
      insert into t3 values ('hi zo', 'pidas', 1)
      insert into t3 values ('coma', 'peredoma', 2)
    EOF
  end

  def emrun
    truncate
    seed
    EM.run {
      set_timer
      yield
    }
  end
end

class MiniTest::Unit::TestCase
  include ::Helper
end
def assert(r, m) raise m unless r end
include ::Helper
