require_relative 'helper'

describe 'DumbConnection' do
  before { Spawn.reseed }
  let(:db) { Tarantool16.new host: 'localhost:16788' }

  it "should select by pk" do
    db.select(:test, 1).must_equal [[1, 'hello', [1,2]]]
    db.select(513, 2).must_equal [[2, 'world', [3,4]]]
  end

  it "should select by pk" do
    db.select(:test, 1).must_equal [[1, 'hello', [1,2]]]
    db.select(513, 2).must_equal [[2, 'world', [3,4]]]
  end

  it "should select by secondary index" do
    db.select(:test, "hello", index: 1).must_equal [[1, 'hello', [1,2]]]
    db.select(:test, "world", index: 'name').must_equal [[2, 'world', [3,4]]]
    db.select(:test, ["hello"], index: 1).must_equal [[1, 'hello', [1,2]]]
  end

  it "should insert" do
    db.insert(:test, [3, "johny", [5,6]])
    db.select(513, 3).must_equal [[3, "johny", [5,6]]]
  end

  it "should update" do
  end

  describe "with field names" do
    before {
      db.define_fields(:test, [[:id, :int], [:name, :str], [:point, :array]])
    }
    it "should select by hash" do
      db.select(:test, {id: 1}).must_equal [{id:1, name:'hello', point:[1,2]}]
      db.select(:test, {name: "world"}).must_equal [{id:2, name:'world', point:[3,4]}]
    end

  end
end
