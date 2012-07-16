require File.expand_path('../helper.rb', __FILE__)

shared_examples_for 'replication and shards' do
  before { TConf.reset_and_up_all }

  let(:t_both) {
    Tarantool.new(type: tarantool_type,
                  servers: [
                    [ TConf.conf(:master1), TConf.conf(:slave1) ],
                    [ TConf.conf(:master2), TConf.conf(:slave2) ]
                  ],
                  replica_strategy: replica_strategy
                 )
  }

  let(:t_first) {
    Tarantool.new(type: tarantool_type,
                  servers: [ TConf.conf(:master1), TConf.conf(:slave1) ],
                  replica_strategy: replica_strategy
                 )
  }

  let(:t_second) {
    Tarantool.new(type: tarantool_type,
                  servers: [ TConf.conf(:master2), TConf.conf(:slave2) ],
                  replica_strategy: replica_strategy
                 )
  }
  let(:replica_strategy) { :master_first }

  HSPACE1_ = {
    fields: {id: :int, name: :str, val: :int},
    keys: :id
  }

  let(:space0_array_both) {
    t_both.space(0, SPACE0[:types], keys: SPACE0[:keys],
                 shard_fields: shard_fields_array0, shard_proc: shard_proc0)
  }
  let(:space0_array_first) { t_first.space(0, SPACE0[:types], keys: SPACE0[:keys]) }
  let(:space0_array_second) { t_second.space(0, SPACE0[:types], keys: SPACE0[:keys]) }
  let(:space0_hash_both) {
    t_both.space_hash(0, HSPACE0[:fields], keys: HSPACE0[:keys],
                      shard_fields: shard_fields_hash0, shard_proc: shard_proc0)
  }
  let(:space0_hash_first) { t_first.space_hash(0, HSPACE0[:fields], keys: HSPACE0[:keys]) }
  let(:space0_hash_second) { t_second.space_hash(0, HSPACE0[:fields], keys: HSPACE0[:keys]) }

  let(:space1_array_both) {
    t_both.space(1, SPACE1[:types], keys: SPACE1[:keys],
                 shard_fields: shard_fields_array1, shard_proc: shard_proc1)
  }
  let(:space1_array_first) { t_first.space(1, SPACE1[:types], keys: SPACE1[:keys]) }
  let(:space1_array_second) { t_second.space(1, SPACE1[:types], keys: SPACE1[:keys]) }
  let(:space1_hash_both) {
    t_both.space_hash(1, HSPACE1_[:fields], keys: HSPACE1_[:keys],
                      shard_fields: shard_fields_hash1, shard_proc: shard_proc1)
  }
  let(:space1_hash_first) { t_first.space_hash(1, HSPACE1_[:fields], keys: HSPACE1_[:keys]) }
  let(:space1_hash_second) { t_second.space_hash(1, HSPACE1_[:fields], keys: HSPACE1_[:keys]) }

  let(:space2_array_both) {
    t_both.space(2, SPACE2[:types], keys: SPACE2[:keys],
                 shard_fields: shard_fields_array2, shard_proc: shard_proc2)
  }
  let(:space2_array_first) { t_first.space(2, SPACE2[:types], keys: SPACE2[:keys]) }
  let(:space2_array_second) { t_second.space(2, SPACE2[:types], keys: SPACE2[:keys]) }
  let(:space2_hash_both) {
    t_both.space_hash(2, HSPACE2[:fields], keys: HSPACE2[:keys],
                      shard_fields: shard_fields_hash2, shard_proc: shard_proc2)
  }
  let(:space2_hash_first) { t_first.space_hash(2, HSPACE2[:fields], keys: HSPACE2[:keys]) }
  let(:space2_hash_second) { t_second.space_hash(2, HSPACE2[:fields], keys: HSPACE2[:keys]) }

  let(:shard_fields_array0) { nil }
  let(:shard_fields_array1) { nil }
  let(:shard_fields_array2) { nil }
  let(:shard_fields_hash0) { nil }
  let(:shard_fields_hash1) { nil }
  let(:shard_fields_hash2) { nil }
  let(:shard_proc0) { nil }
  let(:shard_proc1) { nil }
  let(:shard_proc2) { nil }

  shared_examples_for "array space with simple shard" do
    before {
      blockrun {
        100.times.map{|i|
          space1_array_both.insert([i, "#{i+1}", i+2])
        }
      }
    }
    it "should spread distribution over" do
      results = blockrun{[
        100.times.map{|i|
          space1_array_both.by_pk(i)
        },
        100.times.flat_map{|i|
          space1_array_first.all_by_pks([i])
        },
        100.times.flat_map{|i|
          space1_array_second.all_by_pks([i])
        }
      ]}
      results[1].size.must_equal 50
      results[2].size.must_equal 50
      results[1].include?([50, '51', 52]).must_equal results[2].include?([51, '52', 53])
      (results[1] + results[2]).sort.must_equal results[0]
    end

    it "should delete" do
      results = blockrun{[
        space1_array_both.delete(50, return_tuple: true),
        space1_array_both.by_pk(50),
        space1_array_both.delete(51, return_tuple: true),
        space1_array_both.by_pk(51),
      ]}
      results[0].must_equal [50, '51', 52]
      results[1].must_be_nil
      results[2].must_equal [51, '52', 53]
      results[3].must_be_nil
    end

    it "should update" do
      results = blockrun{[
        space1_array_both.update(50, {1 => '--'}, return_tuple: true),
        space1_array_both.update(51, [[1, :set, '++']], return_tuple: true),
      ]}
      results[0].must_equal [50, '--', 52]
      results[1].must_equal [51, '++', 53]
    end
  end

  describe "array space with default shard" do
    it_behaves_like "array space with simple shard"
  end

  describe "array space with modulo shard" do
    let(:shard_proc1) { :modulo }
    it_behaves_like "array space with simple shard"
  end

  shared_examples_for "hash space with simple shard" do
    before {
      blockrun {
        100.times.map{|i|
          space1_hash_both.insert({id: i, name: "#{i+1}", val: i+2})
        }
      }
    }
    it "should spread distribution over" do
      results = blockrun{[
        100.times.map{|i|
          space1_hash_both.by_pk(i)
        },
        100.times.flat_map{|i|
          space1_hash_first.all_by_pks([i])
        },
        100.times.flat_map{|i|
          space1_hash_second.all_by_pks([i])
        }
      ]}
      results[1].size.must_equal 50
      results[2].size.must_equal 50
      results[1].include?({id:50, name:'51', val:52}).must_equal results[2].include?({id:51, name:'52', val:53})
      (results[1] + results[2]).sort_by{|v| v[:id]}.must_equal results[0]
    end

    it "should delete" do
      results = blockrun{[
        space1_hash_both.delete(50, return_tuple: true),
        space1_hash_both.by_pk(50),
        space1_hash_both.delete(51, return_tuple: true),
        space1_hash_both.by_pk(51),
      ]}
      results[0].must_equal({id: 50, name:'51', val:52})
      results[1].must_be_nil
      results[2].must_equal({id: 51, name:'52', val:53})
      results[3].must_be_nil
    end

    it "should update" do
      results = blockrun{[
        space1_hash_both.update(50, {name: '--'}, return_tuple: true),
        space1_hash_both.update(51, [[:name, :set, '++']], return_tuple: true),
      ]}
      results[0].must_equal({id: 50, name: '--', val: 52})
      results[1].must_equal({id: 51, name: '++', val: 53})
    end
  end

  describe "hash space with default shard" do
    it_behaves_like "hash space with simple shard"
  end

  describe "hash space with modulo shard" do
    let(:shard_proc1) { :modulo }
    it_behaves_like "hash space with simple shard"
  end
end
