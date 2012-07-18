require File.expand_path('../helper.rb', __FILE__)
require 'tarantool/light_record'

describe 'Tarantool::LightRecord::Callback' do
  before { TConf.run(:master1) }
  before { truncate }

  let(:db) { Tarantool.new(TCONFIG.merge(type: :em_fiber)) }
  let(:klass) {
    db = db()
    Class.new(Tarantool::LightRecord) do
      set_tarantool db
      set_space_no 1

      field :id, :int
      field :name, :string
      field :val, :int
    end
  }
  let(:auto_space) { klass.auto_space }
  let(:hash1) { {id: 1, name: 'hello', val: 1} }
  let(:hash2) { {id: 2, name: 'hello', val: 2} }

  it "should be able to insert" do
    emrun(2) {
      auto_space.insert_blk(hash1, &setp(0))
      auto_space.insert_blk(hash2, return_tuple: true, &setp(1))
    }
    results[0].must_equal 1
    results[1].must_be_kind_of klass
    results[1].attributes.must_equal hash2
  end

  describe "manipulations" do
    before {
      emrun(2) {
        auto_space.insert_blk(hash1){emstop}
        auto_space.insert_blk(hash2, return_tuple: true){emstop}
      }
    }
    it "should be able to select" do
      emrun(5) {
        auto_space.select_blk({id: 1}, &setp(0))
        auto_space.select_blk({id: 2}, &setp(1))
        auto_space.select_blk({id: [1,2]}, &setp(2))
        auto_space.select_blk([{id: 1}, {id:2}], &setp(3))
        auto_space.by_pk_blk(1, &setp(4))
      }
      results[0][0].must_be_kind_of klass
      results[0][0].attributes.must_equal hash1
      results[1][0].must_be_kind_of klass
      results[1][0].attributes.must_equal hash2
      results[2].size.must_equal 2
      results[2].map(&:attributes).sort_by{|a| a[:id]}.must_equal [hash1, hash2]
      results[3].size.must_equal 2
      results[3].map(&:attributes).sort_by{|a| a[:id]}.must_equal [hash1, hash2]
      results[4].must_be_kind_of klass
      results[4].attributes.must_equal hash1
    end

    it "should be able to update" do
      emrun(2) {
        auto_space.update_blk(1, {name: "world"}, &setp(0))
        auto_space.update_blk({id:2}, {name: "kill"}, return_tuple: true, &setp(1))
      }
      results[0].must_equal 1
      results[1].must_be_kind_of klass
      results[1].attributes.must_equal hash2.merge(name: "kill")
    end

    it "should be able to delete" do
      emrun(2) {
        auto_space.delete_blk(1, &setp(0))
        auto_space.delete_blk({id:2}, return_tuple: true, &setp(1))
      }
      results[0].must_equal 1
      results[1].must_be_kind_of klass
      results[1].attributes.must_equal hash2
    end

    it "should be able to call" do
      emrun(1) {
        auto_space.call_blk('box.select_range', [0, 100], &setp(0))
      }
      results[0].size.must_equal 2
      results[0][0].must_be_kind_of klass
      results[0][1].must_be_kind_of klass
      results[0].map(&:attributes).sort_by{|a| a[:id]}.must_equal [hash1, hash2]
    end
  end
  
end
