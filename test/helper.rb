require 'minitest/spec'
require 'minitest/autorun'
require 'rr'

require 'tarantool'

class ArrayPackSerializer
  def encode(arr)
    arr.pack("N*")
  end

  def decode(str)
    str.unpack("N*")
  end
end

TCONFIG = { host: '127.0.0.1', port: 33013, admin: 33015 }

SPACE0 = {
  types:  [:str, :str, :str, :int],
  keys:   [0, [1,2], 3]
}
SPACE1 = {
  types:  [:int, :str, :int, 2],
  keys:   0
}
SPACE2 = {
  types:  [:str, :str, :int],
  keys:   [[0,1], 2]
}
SPACE3 = {
  types:  [:int, ArrayPackSerializer.new],
  keys:   [0, 1]
}

HSPACE0 = {
  fields: {name: :str, surname: :str, email: :str, score: :int},
  keys:   [:name, %w{surname email}, 'score']
}
HSPACE1 = {
  fields: {id: :int, _tail: [:str, :int]},
  keys:   :id
}
HSPACE2 = {
  fields: {first: :str, second: :str, third: :int},
  keys:   [%w{first second}, :third]
}
HSPACE3 = {
  fields: {id: :int, scores: ArrayPackSerializer.new},
  keys:   [:id, :scores]
}

module Helper
  def tarantool_pipe
    $tarantool_pipe ||= begin
        cnf = {port: 33013, admin: 33015} #TCONFIG
        tarant = %W{tarantool -p #{cnf[:port]} -m #{cnf[:admin]}}
        tarant = [{}, *tarant, :err => [:child, :out]]
        IO.popen(tarant, 'w+').tap{|p| p.sync = true}
    end
  end

  def exec_tarantool(cmd, lines_to_read)
    cmd = cmd.gsub(/^\s+/, '')
    tarantool_pipe.puts(cmd)
    tarantool_pipe.flush
    lines_to_read.times do
      tarantool_pipe.gets
    end
  end

  def truncate
    exec_tarantool "
       lua truncate(0)
       lua truncate(1)
       lua truncate(2)
       lua truncate(3)
    ", 12
  end

  def seed
    exec_tarantool "
      insert into t0 values ('vasya', 'petrov', 'eb@lo.com', 5)
      insert into t0 values ('ilya', 'zimov', 'il@zi.bot', 13)
      insert into t0 values ('fedor', 'kuklin', 'ku@kl.in', 13)
      insert into t1 values (1, 'common', 4)
      insert into t1 values (2, 'medium', 6, 'common', 7)
      insert into t2 values ('hi zo', 'ho zo', 1)
      insert into t2 values ('hi zo', 'pidas', 1, 3, 5)
      insert into t2 values ('coma', 'peredoma', 2)
    ", 16
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

  def blockrun
    yield
  end

  def mock(u, meth, &block)
    u.define_singleton_method(meth, &block)
  end
end

class MiniTest::Unit::TestCase
  include ::Helper
  include RR::Adapters::MiniTest
end

class << MiniTest::Spec
  def shared_examples
    @shared_examples ||= {}
  end
end

module MiniTest::Spec::SharedExamples
  def shared_examples_for(desc, &block)
    MiniTest::Spec.shared_examples[desc] = block
  end

  def it_behaves_like(desc)
    class_eval &MiniTest::Spec.shared_examples.fetch(desc)
  end
end

Object.class_eval { include(MiniTest::Spec::SharedExamples) }

