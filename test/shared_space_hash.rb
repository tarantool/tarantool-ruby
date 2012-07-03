require File.expand_path('../helper.rb', __FILE__)

shared_examples_for :blocking_hash_space do
  before { clear_db }

  let(:space0) { tarantool.space_hash(0, HSPACE0[:fields], pk: HSPACE0[:pk], indexes: HSPACE0[:indexes])}
  let(:space1) { tarantool.space_hash(1, HSPACE1[:fields], pk: HSPACE1[:pk], indexes: HSPACE1[:indexes])}
  let(:space2) { tarantool.space_hash(2, HSPACE2[:fields], pk: HSPACE2[:pk], indexes: HSPACE2[:indexes])}

  let(:vasya) { {name: 'vasya', surname: 'petrov', email: 'eb@lo.com', score: 5} }
  let(:ilya)  { {name: 'ilya', surname: 'zimov', email: 'il@zi.bot', score: 13} }
  let(:fedor) { {name: 'fedor', surname: 'kuklin', email: 'ku@kl.in', score: 13} }
  let(:hozo)  { {first: 'hi zo', second: 'ho zo', third: 1} }
  let(:pidas) { {first: 'hi zo', second: 'pidas', third: 1, _tail: [3, 5]} }
  let(:peredoma) { {first: 'coma', second: 'peredoma', third: 2} }

  it "should be selectable" do
    results = blockrun { [
      space0.select({name: 'vasya'}, 0, -1),
      space0.select([{name: 'vasya'}], 0, -1),
      space0.select([{name: 'vasya'}, {name: 'ilya'}], 0, -1),
      space0.select([{name: 'ilya'}, {name: 'vasya'}], 0, -1),
      space0.select([{name: 'ilya'}, {name: 'vasya'}], 0, 1),
      space0.select([{name: 'ilya'}, {name: 'vasya'}], 1, 1),
      space0.select({score: 13}, 0, 2),
      space0.select({surname: 'zimov', email: 'il@zi.bot'}, 0, -1),
    ] }
    results[0].must_equal [vasya]
    results[1].must_equal [vasya]
    results[2].must_equal [vasya, ilya]
    results[3].must_equal [ilya, vasya]
    results[4].must_equal [ilya]
    results[5].must_equal [vasya]
    (results[6] - [ilya, fedor]).must_be_empty
    ([ilya, fedor] - results[6]).must_be_empty
    results[7].must_equal [ilya]
  end
  
  it "should select tail (and have #all)" do
    results = blockrun{ [
      space1.all({id: 2}),
      space2.all({third: 1})
    ] }
    results[0].must_equal [{id: 2, _tail: [['medium', 6], ['common', 7]]}]
    (results[1] - [hozo, pidas]).must_be_empty
    ([hozo, pidas] - results[1]).must_be_empty
  end
  
  it "should react on #by_pk" do
    results = blockrun{ [
      space0.by_pk('vasya'),
      space1.by_pk([1]),
      space1.by_pk({id: 2}),
      space2.by_pk(['hi zo', 'ho zo']),
      space2.by_pk({first: 'hi zo', second: 'pidas'})
    ] }
    results[0].must_equal vasya
    results[1].must_equal({id: 1, _tail: [['common', 4]]})
    results[2].must_equal({id: 2, _tail: [['medium', 6], ['common', 7]]})
    results[3].must_equal hozo
    results[4].must_equal pidas
  end

  it "should react on #first" do
    results = blockrun{ [
      space0.first({surname: 'petrov', email: 'eb@lo.com'}),
      space2.first({third: 1})
    ] }
    results[0].must_equal vasya
    [hozo, pidas].must_include results[1]
  end

  it "should insert" do
    petr = {name: 'petr', surname: 'kuprin', email: 'zo@na.ru', score: 1000}
    sp1id3 = {id: 3, _tail: [['no', 8], ['more', 9], ['turtles', 10]]}
    results = blockrun{ [
      space0.insert(petr),
      space0.first(score: 1000),
      space1.insert(sp1id3, return_tuple: true)
    ] }
    results[0].must_equal 1
    results[1].must_equal petr
    results[2].must_equal sp1id3
  end

  it "should replace" do
    huzo = {first: 'hi zo', second: 'ho zo', third: 6, _tail: [5, 4]}
    results = blockrun{ [
      space2.replace(huzo),
      space2.by_pk(['hi zo', 'ho zo']),
      space1.replace({id: 2, _tail: []}, return_tuple: true)
    ] }
    results[0].must_equal 1
    results[1].must_equal huzo
    results[2].must_equal({id: 2})
  end

  it "should update" do
    results = blockrun{ [
      space0.update('vasya', {score: 6}),
      space0.by_pk('vasya'),
      space0.update(['ilya'], {email: 'x@y.z', score: [:+, 2]}, return_tuple: true),
      space1.update({id: 1}, {2 => 'more', 3 => 8}, return_tuple: true),
      space1.update(2, [[2, :insert, 'high'], [2, :ins, 20]], return_tuple: true),
      space2.update(['hi zo', 'pidas'], {_tail: [[:+, 1], [:+, -1]]}, return_tuple: true),
    ] }
    results[0].must_equal 1
    results[1].must_equal vasya.merge(score: 6)
    results[2].must_equal ilya.merge(score: 15, email: 'x@y.z')
    results[3].must_equal({id: 1, _tail: [['common', 4], ['more', 8]]})
    results[4].must_equal({id: 2, _tail: [['medium', 6], ['high', 20], ['common', 7]]})
    results[5].must_equal({first: 'hi zo', second: 'pidas', third: 1, _tail: [4, 4]})
  end

  it "should delete" do
    results = blockrun {[
      space0.delete('vasya', return_tuple: true),
      space0.delete(['ilya'], return_tuple: true),
      space0.delete({name: 'fedor'}, return_tuple: true),
      space1.delete({id: 1}),
      space2.delete(['hi zo', 'ho zo']),
      space2.delete({first: 'hi zo', second: 'pidas'}),
      space2.delete(['unknown', 'man'])
    ]}
    results[0].must_equal vasya
    results[1].must_equal ilya
    results[2].must_equal fedor
    results[3..6].must_equal [1, 1, 1, 0]
  end
  
  it "should be able to call" do
    results = blockrun {[
      space0.call('box.select_range', [0, 2]),
      space0.call('box.select_range', [0, 1000000, 'ilya']),
      space0.call('func2', [1,2], types: [:int, :int], returns: [:str, :int]),
      space0.call('func2', [1,2], types: [:int, :int], returns: {type: :str, val: :int})
    ]} 
    results[0].must_equal [fedor, ilya]
    results[1].must_equal [ilya, vasya]
    results[2].must_equal [['string', 1], ['string', 2]]
    results[3].must_equal [{type: 'string', val: 1}, {type: 'string', val: 2}]
  end

  it "should raise error on wrong key" do
    blockrun {
      proc {
        space2.insert(name: 1)
      }.must_raise Tarantool::ValueError
      proc {
        space2.by_pk(third: 1)
      }.must_raise Tarantool::ValueError
      proc {
        space2.first(name: 1)
      }.must_raise Tarantool::ValueError
      proc {
        space2.all(name: 1)
      }.must_raise Tarantool::ValueError
      proc {
        space2.select([name: 1], 0, 1)
      }.must_raise Tarantool::ValueError
      proc {
        space2.update({third: 1}, {first: 'haha'})
      }.must_raise Tarantool::ValueError
      proc {
        space2.update({first: 'haha'}, {first: 'haha'})
      }.must_raise Tarantool::ValueError
      proc {
        space2.update({first: 'haha', second: 'hoho'}, {name: 'haha'})
      }.must_raise Tarantool::ValueError
    }
  end
end
