require File.expand_path('../helper.rb', __FILE__)

describe 'Tarantool::CallbackDB::SpaceArray' do
  before { TConf.run(:master1) }
  before { clear_db }

  let(:tarantool) { Tarantool.new(TCONFIG.merge(type: :em_callback)) }
  let(:space0) { tarantool.space_array(0, SPACE0[:types], keys: SPACE0[:keys])}
  let(:space1) { tarantool.space_array(1, SPACE1[:types], keys: SPACE1[:keys])}
  let(:space2) { tarantool.space_array(2, SPACE2[:types], keys: SPACE2[:keys])}

  describe "with definition" do
    let(:vasya){ ['vasya', 'petrov', 'eb@lo.com', 5] }
    let(:ilya) { ['ilya', 'zimov', 'il@zi.bot', 13] }
    let(:fedor){ ['fedor', 'kuklin', 'ku@kl.in', 13] }
    it "should be selectable" do
      results = []
      emrun(8) { 
        space0.select(0, 'vasya',                     &setp(results, 0))
        space0.select(0, ['vasya'],                   &setp(results, 1))
        space0.select(0, ['vasya', 'ilya'],           &setp(results, 2))
        space0.select(0, [['vasya'], ['ilya']],       &setp(results, 3))
        space0.select(0, [['ilya'], ['vasya']],       &setp(results, 4))
        space0.select(0, [['ilya'], ['vasya']], 0, 1, &setp(results, 5))
        space0.select(0, [['ilya'], ['vasya']], 1, 1, &setp(results, 6))
        space0.select(2, 13, 0, 2,                    &setp(results, 7))
        space0.select(1, [['zimov','il@zi.bot']],     &setp(results, 8))
      }
      results[0].must_equal [vasya]
      results[1].must_equal [vasya]
      results[2].must_equal [vasya, ilya]
      results[3].must_equal [vasya, ilya]
      results[4].must_equal [ilya, vasya]
      results[5].must_equal [ilya]
      results[6].must_equal [vasya]
      (results[7] - [ilya, fedor]).must_be_empty
      results[8].must_equal [ilya]
    end

    it "should be able to all_by_keys" do
      results = []
      emrun(8) { 
        space0.all_by_keys(0, 'vasya',                         &setp(results, 0))
        space0.all_by_keys(0, ['vasya'],                       &setp(results, 1))
        space0.all_by_keys(0, ['vasya', 'ilya'],               &setp(results, 2))
        space0.all_by_keys(0, [['vasya'], ['ilya']],           &setp(results, 3))
        space0.all_by_keys(0, [['ilya'], ['vasya']],           &setp(results, 4))
        space0.all_by_keys(0, [['ilya'], ['vasya']], limit: 1, &setp(results, 5))
        space0.all_by_keys(0, [['ilya'], ['vasya']], limit: 1, offset: 1, &setp(results, 6))
        space0.all_by_keys(2, 13,                              &setp(results, 7))
        space0.all_by_keys(1, [['zimov','il@zi.bot']],         &setp(results, 8))
      }
      results[0].must_equal [vasya]
      results[1].must_equal [vasya]
      results[2].must_equal [vasya, ilya]
      results[3].must_equal [vasya, ilya]
      results[4].must_equal [ilya, vasya]
      results[5].must_equal [ilya]
      results[6].must_equal [vasya]
      (results[7] - [ilya, fedor]).must_be_empty
      results[8].must_equal [ilya]
    end

    it "should be able to all_by_key" do
      results = []
      emrun(4) { 
        space0.all_by_key(0, 'vasya',               &setp(results, 0))
        space0.all_by_key(0, ['vasya'],             &setp(results, 1))
        space0.all_by_key(2, 13,                    &setp(results, 2))
        space0.all_by_key(1, ['zimov','il@zi.bot'], &setp(results, 3))
      }
      results[0].must_equal [vasya]
      results[1].must_equal [vasya]
      (results[2] - [ilya, fedor]).must_be_empty
      results[3].must_equal [ilya]
    end

    it "should be able to first_by_key" do
      results = []
      emrun(4) { 
        space0.first_by_key(0, 'vasya', &setp(results, 0))
        space0.first_by_key(0, ['ilya'], &setp(results, 1))
        space0.first_by_key(2, 13, &setp(results, 2))
        space0.first_by_key(1, ['petrov','eb@lo.com'], &setp(results, 3))
      }
      results[0].must_equal vasya
      results[1].must_equal ilya
      [ilya, fedor].must_include results[2]
      results[3].must_equal vasya
    end

    it "should be able to by_pk" do
      results = []
      emrun(3) { 
        space0.by_pk('vasya', &setp(results, 0))
        space0.by_pk(['ilya'], &setp(results, 1))
        space2.by_pk(['hi zo', 'ho zo'], &setp(results, 2))
      }
      results[0].must_equal vasya
      results[1].must_equal ilya
      results[2].must_equal ['hi zo', 'ho zo', 1]
    end
    
    it "should raise on not matched pk" do
      proc {
        emrun(1) {
          space0.by_pk(['il','ya'], &setp([], 0))
        }
      }.must_raise Tarantool::ArgumentError
    end

    it "should fetch longer records" do
      results = []
      emrun(2) {
        space2.by_pk(['hi zo', 'pidas'], &setp(results, 0))
        space1.by_pk(2, &setp(results, 1))
      }
      results[0].must_equal ['hi zo', 'pidas', 1, 3, 5]
      results[1].must_equal [2, 'medium', 6, 'common', 7]
    end

    it "should be able to insert" do
      results = []
      asdf = ['asdf', 'asdf', 'asdf', 4, 5]
      qwer = ['qwer', 'qwer', 'qwer', 4, 20, 19]
      zxcv = [4, 'zxcv', 7, 'zxcv', 8]
      xcvb = [5, 'xcvb', 7, 'xcvb', 8]
      emrun(4) {
        space0.insert(asdf, &setp(results, 0))
        space0.insert(qwer, return_tuple: true, &setp(results, 1))
        space1.insert(zxcv){|res|
          results[2] = res
          space1.by_pk(4, &setp(results, 3))
        }
        space1.insert(xcvb, return_tuple: true, &setp(results, 4))
      }
      results[0].must_equal 1
      results[1].must_equal qwer
      results[2].must_equal 1
      results[3].must_equal zxcv
      results[4].must_equal xcvb
    end

    it "should be able to update" do
      results = []
      emrun(6) {
        space0.update('vasya', {1 => 'holodov', 3 => [:+, 2]}, return_tuple: true, &setp(results, 0))
        space0.update('ilya', {[2, :set] => 'we@al.hero', 3 => [:&, 7]}, return_tuple: true, &setp(results, 1))
        space1.update(2, [[2, :^, 3], [4, :|, 20]]){|res|
          results[2] = res
          space1.by_pk(2, &setp(results, 3))
        }
        space1.update(1, [1, :splice, 2, 2, 'nd'], return_tuple: true, &setp(results, 4))
        space2.update(['hi zo', 'pidas'], [[2, :delete], [3, :delete]], return_tuple: true, &setp(results, 5))
        space2.update(['coma', 'peredoma'], [2, :insert, 1]){|res|
          results[6] = res
          space2.all_by_key(1, 1, &setp(results, 7))
        }
      }
      results[0].must_equal ['vasya', 'holodov', 'eb@lo.com', 7]
      results[1].must_equal ['ilya', 'zimov', 'we@al.hero', 5]
      results[2].must_equal 1
      results[3].must_equal [2, 'medium', 5, 'common', 23]
      results[4].must_equal [1, 'condon', 4]
    end

    it "should be able to delete" do
      results = []
      emrun(3) {
        space0.delete('vasya', return_tuple: true, &setp(results, 0))
        space1.delete([1], return_tuple: true, &setp(results, 1))
        space2.delete(['hi zo', 'pidas'], return_tuple: true, &setp(results, 2))
      }
      results[0].must_equal vasya
      results[1].must_equal [1, 'common', 4]
      results[2].must_equal ['hi zo', 'pidas', 1, 3, 5]
    end

    it "should be able to choose index by field numbers" do
      results = []
      emrun(4) {
        space0.first_by_key([0], 'vasya', &setp(results, 0))
        space0.first_by_key([1,2], ['zimov', 'il@zi.bot'], &setp(results, 1))
        space0.all_by_key([3], 13, &setp(results, 2))
        space2.first_by_key([1,0], ['peredoma', 'coma'], &setp(results, 3))
      }
      results[0].must_equal vasya
      results[1].must_equal ilya
      (results[2] - [ilya, fedor]).must_be_empty
      ([ilya, fedor] - results[2]).must_be_empty
      results[3].must_equal ['coma', 'peredoma', 2]
    end
  end
end
