require_relative 'helper'

describe 'DumbConnection' do
  before { Spawn.reseed }
  let(:db) { Tarantool16.new host: 'localhost:16788' }

  it "should select by pk" do
    db.select(:test, 1).must_equal [[1, 'hello', [1,2]]]
    db.select(513, 2).must_equal [[2, 'world', [3,4]]]
  end
end
