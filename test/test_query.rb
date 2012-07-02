require File.expand_path('../helper.rb', __FILE__)

describe EM::Tarantool::Query do
  before { clear_db }

  let(:tarantool) { EM::Tarantool.new(TCONFIG[:host], TCONFIG[:port]) }

  let(:vasya){ ['vasya', 'petrov', 'eb@lo.com', 5] }
  let(:ilya) { ['ilya', 'zimov', 'il@zi.bot', 13] }
  let(:fedor){ ['fedor', 'kuklin', 'ku@kl.in', 13] }
  let(:vasya_h) { {name: 'vasya', surname: 'petrov', email: 'eb@lo.com', score: 5} }
  let(:ilya_h)  { {name: 'ilya', surname: 'zimov', email: 'il@zi.bot', score: 13} }
  let(:fedor_h) { {name: 'fedor', surname: 'kuklin', email: 'ku@kl.in', score: 13} }

  it "should be able to select" do
    results = fibrun {[
      tarantool.select_fib(0, 0, 'vasya', 0, -1),
      tarantool.all_fib(0, 1, ['zimov', 'il@zi.bot'], returns: [:str, :str, :str, :int]),
      tarantool.first_fib(0, 2, 13, returns: {name: :str, surname: :str, email: :str, score: :int}),
      tarantool.all_fib(1, 0, 2, returns: [:int, :str, :int, 2]),
      tarantool.all_fib(1, 0, 2, returns: {id: :int, _tail: [:str, :int]})
    ]}
    vasya_s = vasya.dup
    vasya_s[3] = "\x05\x00\x00\x00"
    results[0].must_equal [vasya_s]
    results[1].must_equal [ilya]
    [ilya_h, fedor_h].must_include results[2]
    results[3].must_equal [[2, 'medium', 6, 'common', 7]]
    results[4].must_equal [{id: 2, _tail: [['medium', 6], ['common', 7]]}]
  end

  it "should insert" do
    results = fibrun {[
      tarantool.insert_fib(0, ['asdf','qwer','zxcv',10], return_tuple: true),
      tarantool.insert_fib(0, ['wert','sdfg','xcvb',1200], types: [:str, :str, :str, :str], return_tuple: true),
      tarantool.insert_fib(1, [3, 'a', 5, 'b', 6], types: [:int, :str, :int, 2], return_tuple: true),
      tarantool.replace_fib(1, [2, 'e', 1]),
      tarantool.first_fib(1, 0, 2, returns: [:int, :str, :int, 2]),
    ]}
    results[0].must_equal ['asdf','qwer','zxcv',10]
    results[1].must_equal ['wert','sdfg','xcvb',"1200"]
    results[2].must_equal [3, 'a', 5, 'b', 6]
    results[3].must_equal 1
    results[4].must_equal [2, 'e', 1]
  end

  it "should update" do
    results = fibrun {[
      tarantool.update_fib(1, 2, [["+2", 4]], returns: [:int, :str, :int, 2], return_tuple: true)
    ]}
    results[0].must_equal [2, 'medium', 10, 'common', 7]
  end

  it "should delete" do
    results = fibrun {[
      tarantool.delete_fib(1, 1, returns: [:int, :str, :int, 2], return_tuple: true),
      tarantool.delete_fib(1, 2, returns: {id: :int, _tail: [:str, :int]}, return_tuple: true)
    ]}
    results[0].must_equal [1, 'common', 4]
    results[1].must_equal({id: 2, _tail: [['medium', 6], ['common', 7]]})
  end

  it "should call" do
    results = fibrun {[
      tarantool.call_fib('func3', ['hello', '2']),
      tarantool.call_fib('func3', ['hello', 2]),
      tarantool.call_fib('func3', [234, 432], returns: [:str, :int]),
      tarantool.call_fib('func3', [234, 432], returns: {type: :str, val: :int}),
    ]}
    results[0].must_equal [['string', 'hello'], ['string', '2']]
    results[1].must_equal [['string', 'hello'], ['string', "\x02\x00\x00\x00"]]
    results[2].must_equal [['string', 234], ['string', 432]]
    results[3].must_equal [{type:'string', val:234}, {type:'string', val:432}]
  end
end
