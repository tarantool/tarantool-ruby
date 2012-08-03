require File.expand_path('../helper.rb', __FILE__)
require 'tarantool/light_record'

shared_examples_for 'resharding' do
  before { TConf.reset_and_up_masters }
  #after { TConf.clear_masters }

  let(:t_before) {
    Tarantool.new(type: tarantool_type,
                  servers: [ [TConf.conf(:master1)], [TConf.conf(:master2)] ],
                  previous_shards_count: 1,
                  insert_to_previous_shard: true)
  }

  let(:t_after) {
    Tarantool.new(type: tarantool_type,
                  servers: [ [TConf.conf(:master1)], [TConf.conf(:master2)] ],
                  previous_shards_count: 1)
  }

  let(:t_past) {
    Tarantool.new(type: tarantool_type,
                  servers: [ [TConf.conf(:master1)], [TConf.conf(:master2)] ])
  }

  let(:t_after) {
    Tarantool.new(type: tarantool_type,
                  servers: [ [TConf.conf(:master1)], [TConf.conf(:master2)] ],
                  previous_shards_count: 1)
  }

  let(:t_first) {
    Tarantool.new(type: tarantool_type, servers: TConf.conf(:master1))
  }

  let(:t_second) {
    Tarantool.new(type: tarantool_type, servers: TConf.conf(:master2))
  }

  HSPACE1_ = {
    fields: {id: :int, name: :str, val: :int},
    keys: :id
  }

  let(:space_array_before) {
    t_before.space(1, SPACE1[:types], keys: SPACE1[:keys], shard_fields: [0], shard_proc: :modulo)
  }
  let(:space_array_after) {
    t_after.space(1, SPACE1[:types], keys: SPACE1[:keys], shard_fields: [0], shard_proc: :modulo)
  }
  let(:space_array_past) {
    t_past.space(1, SPACE1[:types], keys: SPACE1[:keys], shard_fields: [0], shard_proc: :modulo)
  }
  let(:space_array_first) { t_first.space(1, SPACE1[:types], keys: SPACE1[:keys]) }
  let(:space_array_second) { t_second.space(1, SPACE1[:types], keys: SPACE1[:keys]) }

  let(:space_hash_before) {
    t_before.space(1, HSPACE1_[:fields], keys: HSPACE1_[:keys], shard_fields: [:id], shard_proc: :modulo)
  }
  let(:space_hash_after) {
    t_after.space(1, HSPACE1_[:fields], keys: HSPACE1_[:keys], shard_fields: [:id], shard_proc: :modulo)
  }
  let(:space_hash_past) {
    t_past.space(1, HSPACE1_[:fields], keys: HSPACE1_[:keys], shard_fields: [:id], shard_proc: :modulo)
  }
  let(:space_hash_first) { t_first.space(1, HSPACE1_[:fields], keys: HSPACE1_[:keys]) }
  let(:space_hash_second) { t_second.space(1, HSPACE1_[:fields], keys: HSPACE1_[:keys]) }

  shared_examples_for "first step reshard" do
    before do
      blockrun {
        space_first.insert(one)
        space_first.insert(two)
        space_second.insert(five)
      }
    end

    it "should read from both shards" do
      results = blockrun {[
        space_before.by_pk(1),
        space_before.by_pk(2),
        space_before.by_pk(5),
        space_before.all_by_pks([1, 2, 5])
      ]}
      results[0].must_equal one
      results[1].must_equal two
      results[2].must_equal five
      results[3].sort_by(&get_id).must_equal [one, two, five]
    end

    it "should insert into old shard" do
      results = blockrun {
        space_before.insert(three)
        space_before.insert(four)
        [space_first.by_pk(3),
         space_first.by_pk(4),
         space_second.by_pk(3),
         space_second.by_pk(4)
        ]
      }
      results[0].must_equal three
      results[1].must_equal four
      results[2].must_be_nil
      results[3].must_be_nil
    end

    it "should update into both shards" do
      results = blockrun {[
        space_before.update(1, increment),
        space_before.update(5, increment),
        space_first.by_pk(1),
        space_second.by_pk(5)
      ]}
      results[0].must_equal 1
      results[1].must_equal 1
      results[2].must_equal incremented(one)
      results[3].must_equal incremented(five)
    end

    it "should replace into both shards" do
      results = blockrun {[
        space_before.replace(incremented(one)),
        space_before.replace(incremented(five)),
        space_first.by_pk(1),
        space_second.by_pk(5)
      ]}
      results[0].must_equal 1
      results[1].must_equal 1
      results[2].must_equal incremented(one)
      results[3].must_equal incremented(five)
    end

    it "should delete from both shards" do
      results = blockrun {[
        space_before.delete(1, return_tuple: true),
        space_before.delete(5, return_tuple: true),
        space_first.by_pk(1),
        space_second.by_pk(5)
      ]}
      results[0].must_equal one
      results[1].must_equal five
      results[2].must_equal nil
      results[3].must_equal nil
    end
  end

  shared_examples_for "second step reshard" do
    before do
      blockrun {
        space_first.insert(one)
        space_first.insert(two)
        space_second.insert(five)
      }
    end

    it "should read from both shards" do
      results = blockrun {[
        space_after.by_pk(1),
        space_after.by_pk(2),
        space_after.by_pk(5),
        space_after.all_by_pks([1, 2, 5])
      ]}
      results[0].must_equal one
      results[1].must_equal two
      results[2].must_equal five
      results[3].sort_by(&get_id).must_equal [one, two, five]
    end

    it "should insert into right shard" do
      results = blockrun {
        space_after.insert(three)
        space_after.insert(four)
        [space_first.by_pk(3),
         space_first.by_pk(4),
         space_second.by_pk(3),
         space_second.by_pk(4)
        ]
      }
      results[0].must_be_nil
      results[1].must_equal four
      results[2].must_equal three
      results[3].must_be_nil
    end

    it "should update into both shards" do
      results = blockrun {[
        space_after.update(1, increment),
        space_after.update(5, increment),
        space_first.by_pk(1),
        space_second.by_pk(5)
      ]}
      results[0].must_equal 1
      results[1].must_equal 1
      results[2].must_equal incremented(one)
      results[3].must_equal incremented(five)
    end

    it "should replace into both shards" do
      results = blockrun {[
        space_after.replace(incremented(one)),
        space_after.replace(incremented(five)),
        space_first.by_pk(1),
        space_second.by_pk(5)
      ]}
      results[0].must_equal 1
      results[1].must_equal 1
      results[2].must_equal incremented(one)
      results[3].must_equal incremented(five)
    end

    it "should delete from both shards" do
      results = blockrun {[
        space_after.delete(1, return_tuple: true),
        space_after.delete(5, return_tuple: true),
        space_first.by_pk(1),
        space_second.by_pk(5)
      ]}
      results[0].must_equal one
      results[1].must_equal five
      results[2].must_equal nil
      results[3].must_equal nil
    end
  end

  describe "array space reshard" do
    let(:space_before) { space_array_before }
    let(:space_after) { space_array_after }
    let(:space_past) { space_array_past }
    let(:space_first) { space_array_first }
    let(:space_second) { space_array_second }

    let(:one) { [1, '1', 1] }
    let(:two) { [2, '2', 2] }
    let(:three) { [3, '3', 3] }
    let(:four) { [4, '4', 4] }
    let(:five) { [5, '5', 5] }
    let(:increment) { {2 => [:+, 1] } }
    def incremented(tuple)
      tuple.dup.tap{|t| t[2]+=1}
    end
    def get_id
      proc{|tuple| tuple[0]}
    end

    describe "first step" do
      it_behaves_like "first step reshard"
    end

    describe "second step" do
      it_behaves_like "second step reshard"
    end
  end

  describe "hash space reshard" do
    let(:space_before) { space_hash_before }
    let(:space_after) { space_hash_after }
    let(:space_past) { space_hash_past }
    let(:space_first) { space_hash_first }
    let(:space_second) { space_hash_second }

    def tuple_by_i(i)
      {id: i, name: i.to_s, val: i}
    end

    let(:one) { tuple_by_i(1) }
    let(:two) { tuple_by_i(2) }
    let(:three) { tuple_by_i(3) }
    let(:four) { tuple_by_i(4) }
    let(:five) { tuple_by_i(5) }
    let(:increment) { {val: [:+, 1] } }
    def incremented(tuple)
      tuple.dup.tap{|t| t[:val]+=1}
    end
    def get_id
      proc{|tuple| tuple[:id]}
    end


    describe "first step" do
      it_behaves_like "first step reshard"
    end

    describe "second step" do
      it_behaves_like "second step reshard"
    end
  end

end
