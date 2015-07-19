require_relative 'helper'

describe 'DumbConnection' do
  before { Spawn.reseed }
  let(:db) { Tarantool16.new host: 'localhost:16788' }
  let(:r1) { [1, 'hello', [1,2]].deep_freeze }
  let(:r2) { [2, 'world', [3,4]].deep_freeze }

  it "should select by pk" do
    db.select(:test, 1).must_equal [r1]
    db.select(513, 2).must_equal [r2]
  end

  it "should select by secondary index" do
    db.select(:test, "hello", index: 1).must_equal [r1]
    db.select(:test, "world", index: 'name').must_equal [r2]
    db.select(:test, ["hello"], index: 1).must_equal [r1]

    db.select(:test, [[1,2]], index: 2).must_equal [r1]
  end

  it "should iterate" do
    db.select(:test, 1, iterator: :>=, index: 3).must_equal [r1, r2]
    db.select(:test, 1, iterator: :>, index: 3).must_equal [r2]
    db.select(:test, 1, iterator: :<, index: 3).must_equal []
    db.select(:test, 1, iterator: :<=, index: 3).must_equal [r1]
    db.select(:test, 2, iterator: :>=, index: 3).must_equal [r2]
    db.select(:test, 2, iterator: :>, index: 3).must_equal []
    db.select(:test, 2, iterator: :<, index: 3).must_equal [r1]
    db.select(:test, 2, iterator: :<=, index: 3).must_equal [r2, r1]

    db.select(:test, 'hello', index: 1, iterator: :>).must_equal [r2]

    db.select(:test, [[0,0]], index: 2, iterator: "<->").must_equal [r1, r2]
    db.select(:test, [[4,4]], index: 2, iterator: "<->").must_equal [r2, r1]
  end

  it "should insert" do
    db.insert(:test, [4, "johny", [5,6]])
    db.select(513, 4).must_equal [[4, "johny", [5,6]]]
  end

  describe "with field names" do
    before {
      db.define_fields(:test, [[:id, :int], [:name, :str], [:point, :array]])
    }
    let(:h1){ {id:1, name:'hello', point:[1,2]}.deep_freeze }
    let(:h2){ {id:2, name:'world', point:[3,4]}.deep_freeze }

    it "should select by hash" do
      db.select(:test, {id: 1}).must_equal [h1]
      db.select(:test, {name: "world"}).must_equal [h2]
    end

    it "should iterate" do
      db.select(:test, {id: 0}, iterator: :>).must_equal [h1, h2]
      db.select(:test, {id: 4}, iterator: :<).must_equal [h2, h1]
    end
  end
end
