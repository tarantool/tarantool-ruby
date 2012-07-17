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
    let(:space_both)  { space1_array_both }
    let(:space_first) { space1_array_first }
    let(:space_second){ space1_array_second }
    before {
      blockrun {
        100.times.map{|i|
          space_both.insert([i, "#{i+1}", i+2])
        }
      }
    }
    it "should spread distribution over" do
      all_pks = 100.times.map{|i| [i]}
      results = blockrun{[
        100.times.map{|i|      space_both.by_pk(i) },
        space_first.all_by_pks(all_pks),
        space_second.all_by_pks(all_pks),
        space_both.all_by_pks(all_pks),
      ]}
      results[1].size.must_equal 50
      results[2].size.must_equal 50
      results[1].include?([50, '51', 52]).must_equal results[2].include?([51, '52', 53])
      (results[1] + results[2]).sort.must_equal results[0]
      results[3].sort.must_equal results[0]
    end

    it "should delete" do
      results = blockrun{[
        space_both.delete(50, return_tuple: true),
        space_both.by_pk(50),
        space_both.delete(51, return_tuple: true),
        space_both.by_pk(51),
      ]}
      results[0].must_equal [50, '51', 52]
      results[1].must_be_nil
      results[2].must_equal [51, '52', 53]
      results[3].must_be_nil
    end

    it "should update" do
      results = blockrun{[
        space_both.update(50, {1 => '--'}, return_tuple: true),
        space_both.update(51, [[1, :set, '++']], return_tuple: true),
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
    let(:space_both)  { space1_hash_both }
    let(:space_first) { space1_hash_first }
    let(:space_second){ space1_hash_second }
    before {
      blockrun {
        100.times.map{|i|
          space_both.insert({id: i, name: "#{i+1}", val: i+2})
        }
      }
    }
    it "should spread distribution over" do
      all_pks = 100.times.map{|i| [i]}
      results = blockrun{[
        100.times.map{|i|      space_both.by_pk(i) },
        space_first.all_by_pks(all_pks),
        space_second.all_by_pks(all_pks),
        space_both.all_by_pks(all_pks),
      ]}
      results[1].size.must_equal 50
      results[2].size.must_equal 50
      results[1].include?({id:50, name:'51', val:52}).must_equal results[2].include?({id:51, name:'52', val:53})
      (results[1] + results[2]).sort_by{|v| v[:id]}.must_equal results[0]
      results[3].sort_by{|v| v[:id]}.must_equal results[0]
    end

    it "should delete" do
      results = blockrun{[
        space_both.delete(50, return_tuple: true),
        space_both.by_pk(50),
        space_both.delete(51, return_tuple: true),
        space_both.by_pk(51),
      ]}
      results[0].must_equal({id: 50, name:'51', val:52})
      results[1].must_be_nil
      results[2].must_equal({id: 51, name:'52', val:53})
      results[3].must_be_nil
    end

    it "should update" do
      results = blockrun{[
        space_both.update(50, {name: '--'}, return_tuple: true),
        space_both.update(51, [[:name, :set, '++']], return_tuple: true),
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

  shared_examples_for "array space shard with composit pk" do
    def iii(pk) [*pk, pk.hash & 0xffffff] end
    let(:space_both){ space2_array_both }
    let(:space_first){ space2_array_first }
    let(:space_second){ space2_array_second }
    let(:pks) {
      (1..10).to_a.product((1..10).to_a).map{|i,j| [i.to_s, j.to_s]}
    }
    let(:pk_first) {
      pks.detect{|pk| space_both._detect_shards_for_key(pk, 0) == 0}
    }
    let(:pk_second) {
      pks.detect{|pk| space_both._detect_shards_for_key(pk, 0) == 1}
    }
    before {
      blockrun{
        pks.each{|pk| space_both.insert(iii(pk))}
      }
    }

    it "should spread distribution over" do
      results = blockrun{[
        pks.map{|pk|      space_both.by_pk(pk)},
        pks.flat_map{|pk| space_first.all_by_pks([pk])},
        pks.flat_map{|pk| space_second.all_by_pks([pk])},
        space_first.by_pk(pk_first),
        space_second.by_pk(pk_second),
        space_first.by_pk(pk_second),
        space_second.by_pk(pk_first),
      ]}
      results[0].compact.size.must_equal pks.size
      (results[1] + results[2]).sort.must_equal results[0].sort
      results[1].size.must_be_close_to pks.size/2, pks.size/5
      results[2].size.must_be_close_to pks.size/2, pks.size/5
      results[3].must_equal iii(pk_first)
      results[4].must_equal iii(pk_second)
      results[5].must_equal nil
      results[6].must_equal nil
    end

    it "should delete" do
      results = blockrun{[
        space_both.delete(pk_first, return_tuple:true),
        space_both.by_pk(pk_first),
        space_both.delete(pk_second, return_tuple:true),
        space_both.by_pk(pk_second),
      ]}
      results[0].must_equal iii(pk_first)
      results[1].must_be_nil
      results[2].must_equal iii(pk_second)
      results[3].must_be_nil
    end

    it "should update" do
      results = blockrun{[
        space_both.update(pk_first, {2=> [:+, 1]}, return_tuple:true),
        space_both.update(pk_second, {2=> [:+, 1]}, return_tuple:true),
      ]}
      results[0][2].must_equal iii(pk_first)[2]+1
      results[1][2].must_equal iii(pk_second)[2]+1
    end

    it "should search by keys half" do
      results = blockrun{[
        pks.map{|pk| space_both.by_pk(pk)},
        (1..10).flat_map{|i| space_both.select(0, [i.to_s])},
        space_both.select(0, (1..10).map{|i| [i.to_s]})
      ]}
      results[1].sort.must_equal results[0].sort
      results[2].sort.must_equal results[0].sort
    end
  end

  describe "array space default shard with composit pk" do
    it_behaves_like "array space shard with composit pk"
  end

  describe "array space custom shard with composit pk" do
    class ShardProc2
      attr :count
      def initialize
        @count = 0
      end
      def call(shard_values, shards_count, this)
        @count += 1
        shard_values[0] && shard_values[0].to_i % shards_count
      end
    end
    let(:shard_proc2){ ShardProc2.new }

    it_behaves_like "array space shard with composit pk"

    it "should call custom shard proc" do
      shard_proc2.count.must_equal pks.size
    end
  end

  shared_examples_for "hash space shard with composit pk" do
    def iii(pk) {first: pk[0], second: pk[1], third: pk.hash & 0xffffff} end
    let(:space_both){ space2_hash_both }
    let(:space_first){ space2_hash_first }
    let(:space_second){ space2_hash_second }
    let(:pks) {
      (1..10).to_a.product((1..10).to_a).map{|i,j| [i.to_s, j.to_s]}
    }
    let(:pk_first) {
      pks.detect{|pk| space_both._detect_shards_for_key(pk, 0) == 0}
    }
    let(:pk_second) {
      pks.detect{|pk| space_both._detect_shards_for_key(pk, 0) == 1}
    }
    before {
      blockrun{
        pks.each{|pk| space_both.insert(iii(pk))}
      }
    }

    it "should spread distribution over" do
      results = blockrun{[
        pks.map{|pk|      space_both.by_pk(pk)},
        pks.flat_map{|pk| space_first.all_by_pks([pk])},
        pks.flat_map{|pk| space_second.all_by_pks([pk])},
        space_first.by_pk(pk_first),
        space_second.by_pk(pk_second),
        space_first.by_pk(pk_second),
        space_second.by_pk(pk_first),
      ]}
      results[0].compact.size.must_equal pks.size
      (results[1] + results[2]).sort_by(&:values).must_equal results[0].sort_by(&:values)
      results[1].size.must_be_close_to pks.size/2, pks.size/5
      results[2].size.must_be_close_to pks.size/2, pks.size/5
      results[3].must_equal iii(pk_first)
      results[4].must_equal iii(pk_second)
      results[5].must_equal nil
      results[6].must_equal nil
    end

    it "should delete" do
      results = blockrun{[
        space_both.delete(pk_first, return_tuple:true),
        space_both.by_pk(pk_first),
        space_both.delete(pk_second, return_tuple:true),
        space_both.by_pk(pk_second),
      ]}
      results[0].must_equal iii(pk_first)
      results[1].must_be_nil
      results[2].must_equal iii(pk_second)
      results[3].must_be_nil
    end

    it "should update" do
      results = blockrun{[
        space_both.update(pk_first, {third: [:+, 1]}, return_tuple:true),
        space_both.update(pk_second, {third: [:+, 1]}, return_tuple:true),
      ]}
      results[0][:third].must_equal iii(pk_first)[:third]+1
      results[1][:third].must_equal iii(pk_second)[:third]+1
    end

    it "should search by keys half" do
      results = blockrun{[
        pks.map{|pk| space_both.by_pk(pk)},
        (1..10).flat_map{|i| space_both.select({first: i.to_s})},
        space_both.select((1..10).map{|i| {first: i.to_s}})
      ]}
      results[1].sort_by(&:values).must_equal results[0].sort_by(&:values)
      results[2].sort_by(&:values).must_equal results[0].sort_by(&:values)
    end
  end

  describe "hash space default shard with composit pk" do
    it_behaves_like "hash space shard with composit pk"
  end

  describe "array space with shard on not pk" do
    let(:shard_fields_array0) { [3] }
    let(:space_both){ space0_array_both }
    let(:space_first){ space0_array_first }
    let(:space_second){ space0_array_second }
    let(:pks){ 100.times.to_a }

    before {
      blockrun{
        pks.each{|i| space_both.insert([i, i+1, i+2, i/10]) }
      }
    }

    it "should spread distribution over" do
      results = blockrun{[
        pks.flat_map{|i| space_both.all_by_pks([i])},
        pks.flat_map{|i| space_first.all_by_pks([i])},
        pks.flat_map{|i| space_second.all_by_pks([i])},
        (0..9).flat_map{|i| space_both.select(2, i)},
        (0..9).flat_map{|i| space_first.select([3], i)},
        (0..9).flat_map{|i| space_second.select(2, i)},
        space_both.all_by_pks(pks.map{|pk| [pk]}),
        space_both.select(2, (0..9).map{|i| [i]})
      ]}
      results[0].size.must_equal pks.size
      results[1].size.must_equal pks.size/2
      results[2].size.must_equal pks.size/2
      (results[1]+results[2]).sort_by{|v|v[0].to_i}.must_equal results[0]

      results[3].size.must_equal pks.size
      results[3].sort_by{|v|v[0].to_i}.must_equal results[0]
      results[4].size.must_equal pks.size/2
      results[5].size.must_equal pks.size/2
      (results[4]+results[5]).sort_by{|v|v[0].to_i}.must_equal results[0]

      results[4].include?(['21','22','23',2]).must_equal(
        results[5].include?(['31','32','33',3]))

      results[6].sort_by{|v|v[0].to_i}.must_equal results[0]
      results[7].sort_by{|v|v[0].to_i}.must_equal results[0]
    end

    it "should delete" do
      results = blockrun{[
        space_both.delete('21', return_tuple:true),
        space_both.by_pk('21'),
        space_both.delete('31', return_tuple:true),
        space_both.by_pk('31')
      ]}
      results[0].must_equal ['21','22','23',2]
      results[1].must_be_nil
      results[2].must_equal ['31','32','33',3]
      results[3].must_be_nil
    end

    it "should update" do
      results = blockrun{[
        space_both.update('21', {1=> [:splice,3,0,'!']}, return_tuple:true),
        space_both.update('31', {1=> [:splice,3,0,'!']}, return_tuple:true),
      ]}
      results[0].must_equal ['21','22!','23',2]
      results[1].must_equal ['31','32!','33',3]
    end
  end

  describe "hash space with shard on not pk" do
    let(:shard_fields_hash0) { [:score] }
    let(:space_both){ space0_hash_both }
    let(:space_first){ space0_hash_first }
    let(:space_second){ space0_hash_second }
    let(:pks){ 100.times.to_a }

    before {
      blockrun{
        pks.each{|i|
          space_both.insert(name: i, surname: i+1, email: i+2, score: i/10)
        }
      }
    }
    let(:twenty_one){ {name:'21',surname:'22',email:'23',score: 2} }
    let(:thirty_one){ {name:'31',surname:'32',email:'33',score: 3} }

    it "should spread distribution over" do
      results = blockrun{[
        pks.flat_map{|i| space_both.all_by_pks([i])},
        pks.flat_map{|i| space_first.all_by_pks([i])},
        pks.flat_map{|i| space_second.all_by_pks([i])},
        (0..9).flat_map{|i| space_both.select(score: i)},
        (0..9).flat_map{|i| space_first.select(score: i)},
        (0..9).flat_map{|i| space_second.select(score: i)},
        space_both.all_by_pks(pks.map{|pk| [pk]}),
        space_both.select(score: (0..9).to_a)
      ]}
      results[0].size.must_equal pks.size
      results[1].size.must_equal pks.size/2
      results[2].size.must_equal pks.size/2
      (results[1]+results[2]).sort_by{|v|v[:name].to_i}.must_equal results[0]

      results[3].size.must_equal pks.size
      results[3].sort_by{|v|v[:name].to_i}.must_equal results[0]
      results[4].size.must_equal pks.size/2
      results[5].size.must_equal pks.size/2
      (results[4]+results[5]).sort_by{|v|v[:name].to_i}.must_equal results[0]

      results[4].include?(twenty_one).must_equal results[5].include?(thirty_one)

      results[6].sort_by{|v|v[:name].to_i}.must_equal results[0]
      results[7].sort_by{|v|v[:name].to_i}.must_equal results[0]
    end

    it "should delete" do
      results = blockrun{[
        space_both.delete('21', return_tuple:true),
        space_both.by_pk('21'),
        space_both.delete('31', return_tuple:true),
        space_both.by_pk('31')
      ]}
      results[0].must_equal twenty_one
      results[1].must_be_nil
      results[2].must_equal thirty_one
      results[3].must_be_nil
    end

    it "should update" do
      results = blockrun{[
        space_both.update('21', {surname: [:splice,3,0,'!']}, return_tuple:true),
        space_both.update('31', {surname: [:splice,3,0,'!']}, return_tuple:true),
      ]}
      results[0].must_equal twenty_one.merge(surname: '22!')
      results[1].must_equal thirty_one.merge(surname: '32!')
    end
  end

  shared_examples_for "space test call" do
    before {
      blockrun {
        space_both.insert(one)
        space_both.insert(two)
      }
    }

    it "should call on both" do
      result = blockrun{ space_both.call('box.select_range', [0, 100]) }
      result.sort_by{|t| get_id(t)}.must_equal [one, two]
    end

    it "should call on specified" do
      results = blockrun{[
        space_both.call('box.select_range', [0, 100], shard_key: 1),
        space_both.call('box.select_range', [0, 100], shard_keys: [2])
      ]}
      results[0].must_equal [one]
      results[1].must_equal [two]
    end
  end

  describe "space array test call" do
    let(:space_both){ space1_array_both }
    let(:space_first){ space1_array_first }
    let(:space_second){ space1_array_second }

    let(:one) { [1, 'a', 1] }
    let(:two) { [2, 'b', 2] }
    def get_id(tuple) tuple[0] end

    it_behaves_like "space test call"
  end

  describe "space hash test call" do
    let(:space_both){ space1_hash_both }
    let(:space_first){ space1_hash_first }
    let(:space_second){ space1_hash_second }

    let(:one) { {id: 1, name: 'a', val: 1} }
    let(:two) { {id: 2, name: 'b', val: 2} }
    def get_id(tuple) tuple[:id] end

    it_behaves_like "space test call"
  end

  shared_examples_for "explicit shard number" do
    before {
      blockrun {
        space_both.shard(0).insert(one)
        space_both.shard(1).insert(two)
      }
    }

    it "should not find under implicit shard" do
      blockrun{[
        space_both.by_pk(1),
        space_both.by_pk(2),
      ]}.must_equal [nil, nil]
    end

    it "should not find under explicit but not same shard" do
      blockrun{[
        space_both.shard(1).by_pk(1),
        space_both.shard(0).by_pk(2),
      ]}.must_equal [nil, nil]
    end

    it "should find under explict shard" do
      blockrun{[
        space_both.shard(0).by_pk(1),
        space_both.shard(1).by_pk(2)
      ]}.must_equal [one, two]
    end

    it "should call on both" do
      result = blockrun{ space_both.call('box.select_range', [0, 100]) }
      result.sort_by{|t| get_id(t)}.must_equal [one, two]
    end

    it "should call on specified" do
      results = blockrun{[
        space_both.shard(1).call('box.select_range', [0, 100]),
        space_both.shard(0).call('box.select_range', [0, 100])
      ]}
      results[0].must_equal [two]
      results[1].must_equal [one]
    end
  end

  describe "space array explicit shard number" do
    let(:space_both){ space1_array_both }
    let(:space_first){ space1_array_first }
    let(:space_second){ space1_array_second }

    let(:one) { [1, 'a', 1] }
    let(:two) { [2, 'b', 2] }
    def get_id(tuple) tuple[0] end

    it_behaves_like "explicit shard number"
  end

  describe "space hash explicit shard number" do
    let(:space_both){ space1_hash_both }
    let(:space_first){ space1_hash_first }
    let(:space_second){ space1_hash_second }

    let(:one) { {id: 1, name: 'a', val: 1} }
    let(:two) { {id: 2, name: 'b', val: 2} }
    def get_id(tuple) tuple[:id] end

    it_behaves_like "explicit shard number"
  end

  shared_examples_for "replication tests" do
    before{
      blockrun{
        space.insert(record1)
        space.insert(record2)
      }
      sleep 0.12
      stop_masters
    }

    it "should read from slave" do
      results = blockrun{[
        space.by_pk(2),
        space.by_pk(1),
        space.call('box.select_range', [0, 100])
      ]}
      results[1].must_equal record1
      results[0].must_equal record2
      results[2].sort_by{|v| get_id(v)}.must_equal [record1, record2]
    end

    it "should raise exception when slave had not become master" do
      blockrun{
        [1,2].each do |i|
          proc{
            p space.delete(i)
          }.must_raise Tarantool::NoMasterError
          proc{
            space.update(i, update_op)
          }.must_raise Tarantool::NoMasterError
          proc{
            space.call('box.delete', [0, i])
          }.must_raise Tarantool::NoMasterError
        end
      }
    end

    it "should perform write when slave became master" do
      make_masters
      results = blockrun{[
        space.call('box.select_range', [0, 100]),
        space.update(1, update_op, return_tuple: true),
        space.delete(1, return_tuple: true),
        space.update(2, update_op, return_tuple: true),
        space.delete(2, return_tuple: true),
      ]}
      results[0].sort_by{|v| get_id(v)}.must_equal [record1, record2]
      results[1].must_equal updated1
      results[2].must_equal updated1
      results[3].must_equal updated2
      results[4].must_equal updated2
    end

    it "should perform read from previous masters when slaves (which became masters) fails" do
      make_masters
      make_slaves
      sleep 0.15
      stop_masters :slaves

      results = blockrun{[
        space.by_pk(1),
        space.by_pk(2),
        space.call('box.select_range', [0, 100])
      ]}
      results[0].must_equal record1
      results[1].must_equal record2
      results[2].sort_by{|v| get_id(v)}.must_equal [record1, record2]
    end

    it "should fail when masters and slaves both down" do
      stop_masters(:slaves)
      blockrun{
        proc {
          space.by_pk(1)
        }.must_raise ::Tarantool::ConnectionError
      }
    end
  end

  shared_examples_for "space array replication" do
    let(:record1){ [1, 'a', 1] }
    let(:record2){ [2, 'a', 2] }
    let(:update_op){ {1 => 'b'} }
    let(:updated1){ [1, 'b', 1] }
    let(:updated2){ [2, 'b', 2] }
    def get_id(v) v[0] end
  end

  shared_examples_for "space hash replication" do
    let(:record1){ {id: 1, name: 'a', val: 1} }
    let(:record2){ {id: 2, name: 'a', val: 2} }
    let(:update_op){ {name: 'b'} }
    let(:updated1){ {id: 1, name: 'b', val: 1} }
    let(:updated2){ {id: 2, name: 'b', val: 2} }
    def get_id(v) v[:id] end
  end

  shared_examples_for "single replication" do
    def stop_masters(which=:masters)
      if which == :masters
        TConf.stop(:master1)
      else
        TConf.stop(:slave1)
      end
    end
    def make_masters(which=:masters)
      if which == :masters
        TConf.promote_to_master(:slave1)
      else
        TConf.promote_to_master(:master1)
      end
    end
    def make_slaves
      TConf.promote_to_slave(:master1, :slave1)
    end
  end

  shared_examples_for "shard and replication" do
    def stop_masters(which=:masters)
      if which == :masters
        TConf.stop(:master1)
        TConf.stop(:master2)
      else
        TConf.stop(:slave1)
        TConf.stop(:slave2)
      end
    end
    def make_masters(which=:masters)
      if which == :masters
        TConf.promote_to_master(:slave1)
        TConf.promote_to_master(:slave2)
      else
        TConf.promote_to_master(:master1)
        TConf.promote_to_master(:master2)
      end
    end
    def make_slaves
      TConf.promote_to_slave(:master1, :slave1)
      TConf.promote_to_slave(:master2, :slave2)
    end
  end

  describe "space array single replication" do
    let(:space){ space1_array_first }

    it_behaves_like "space array replication"
    it_behaves_like "single replication"
    it_behaves_like "replication tests"
  end

  describe "space hash single replication" do
    let(:space){ space1_hash_first }

    it_behaves_like "space hash replication"
    it_behaves_like "single replication"
    it_behaves_like "replication tests"
  end

  describe "space hash shard and replication" do
    let(:space){ space1_hash_both }

    it_behaves_like "space hash replication"
    it_behaves_like "shard and replication"
    it_behaves_like "replication tests"
  end

  describe "space array shard and replication" do
    let(:space){ space1_array_both }

    it_behaves_like "space array replication"
    it_behaves_like "shard and replication"
    it_behaves_like "replication tests"
  end

end
