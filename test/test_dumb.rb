require_relative 'helper'

describe 'DumbConnection' do
  before { Spawn.reseed }
  let(:db) { Tarantool16.new host: 'localhost:16788', timeout: 0.1 }
  let(:r1) { [1, 'hello', [1,2], 100].deep_freeze }
  let(:r2) { [2, 'world', [3,4], 200].deep_freeze }
  let(:r3) { [3, 'wicky', [7,10], 300].deep_freeze }

  def rwith(r, fld, val)
    r=r.dup
    r[fld]=val
    r
  end

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

  it "should iterate with offset and limit" do
    db.insert(:test, r3)
    db.select(:test, 1, iterator: :>=, index: 3, limit: 1).must_equal [r1]
    db.select(:test, 1, iterator: :>=, index: 3, offset: 1).must_equal [r2, r3]
    db.select(:test, 1, iterator: :>=, index: 3, limit: 1, offset: 1).must_equal [r2]
  end

  it "should insert" do
    db.insert(:test, r3)
    db.select(513, 3).must_equal [r3]
  end

  it "should update" do
    db.update(:test, 1, [[:+, 3, 1]]).must_equal [rwith(r1,3,101)]
    db.update(:test, "world", {1 => [':', 6, 0, '!']}, index: 1).must_equal [rwith(r2,1,"world!")]
  end

  it "should delete" do
    db.delete(:test, 1).must_equal [r1]
    db.delete(:test, "world", index: 1).must_equal [r2]
  end

  it "should eval" do
    db.eval("local a, b = ... ; return a + b", [1, 2]).must_equal [3]
  end

  describe "with field names" do
    before {
      db.define_fields(:test, [[:id, :int], [:name, :str], [:point, :array], [:count, :int]])
    }
    let(:h1){ {id:1, name:'hello', point:[1,2], count:100}.deep_freeze }
    let(:h2){ {id:2, name:'world', point:[3,4], count:200}.deep_freeze }
    let(:h3) { {id:3, name:'wicky', point:[7,10], count:300}.deep_freeze }

    def hwith(h, fld, val)
      h=h.dup
      h[fld] = val
      h
    end

    it "should select by hash" do
      db.select(:test, {id: 1}).must_equal [h1]
      db.select(:test, {name: "world"}).must_equal [h2]
    end

    it "should iterate" do
      db.select(:test, {id: 0}, iterator: :>).must_equal [h1, h2]
      db.select(:test, {id: 4}, iterator: :<).must_equal [h2, h1]
      db.select(:test, {name: 'hello'}, iterator: :>).must_equal [h2]
      db.select(:test, {point: [0,0]}, iterator: "<->").must_equal [h1, h2]
      db.select(:test, {point: [4,4]}, iterator: "<->").must_equal [h2, h1]
    end

    it "should insert" do
      db.insert(:test, h3)
      db.select(513, {id:3}).must_equal [h3]
    end

    it "should update" do
      db.update(:test, {id: 1}, [[:+, :count, 1]]).must_equal [hwith(h1, :count, 101)]
      db.update(:test, {name: "world"}, {count:  [:+, 1]}).must_equal [hwith(h2, :count, 201)]
    end
  end

  describe "timeouts" do
    it "should timeout on connect" do
      Spawn.with_pause do
        proc {
          db.get(:test, 1)
        }.must_raise Tarantool16::Connection::CouldNotConnect
      end
    end

    it "should timeout on request" do
      db.get(:test, 1)
      Spawn.with_pause do
        proc {
          db.get(:test, 1)
        }.must_raise Tarantool16::Connection::Timeout
      end
    end
  end
end
