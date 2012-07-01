require File.expand_path('../helper.rb', __FILE__)

describe EM::Tarantool::SpaceFiber do
  before { clear_db }

  let(:tarantool) { EM::Tarantool.new(TCONFIG[:host], TCONFIG[:port]) }
  let(:space0) { tarantool.space_fiber(0, SPACE0[:types], pk: SPACE0[:pk], indexes: SPACE0[:indexes])}
  let(:space1) { tarantool.space_fiber(1, SPACE1[:types], pk: SPACE1[:pk], indexes: SPACE1[:indexes])}
  let(:space2) { tarantool.space_fiber(2, SPACE2[:types], pk: SPACE2[:pk], indexes: SPACE2[:indexes])}

  describe "with definition" do
    let(:vasya){ ['vasya', 'petrov', 'eb@lo.com', 5] }
    let(:ilya) { ['ilya', 'zimov', 'il@zi.bot', 13] }
    let(:fedor){ ['fedor', 'kuklin', 'ku@kl.in', 13] }
    it "should be selectable" do
      results = fibrun { [
        space0.select(0, 0, -1, 'vasya'),
        space0.select(0, 0, -1, ['vasya']),
        space0.select(0, 0, -1, ['vasya', 'ilya']),
        space0.select(0, 0, -1, [['vasya'], ['ilya']]),
        space0.select(0, 0, -1, [['ilya'], ['vasya']]),
        space0.select(0, 0, 1, [['ilya'], ['vasya']]),
        space0.select(0, 1, 1, [['ilya'], ['vasya']]),
        space0.select(2, 0, 2, 13),
        space0.select(1, 0, -1, [['zimov','il@zi.bot']])
      ] }
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
      results = fibrun { [
        space0.all_by_keys(0, 'vasya'),
        space0.all_by_keys(0, ['vasya']),
        space0.all_by_keys(0, ['vasya', 'ilya']),
        space0.all_by_keys(0, [['vasya'], ['ilya']]),
        space0.all_by_keys(0, [['ilya'], ['vasya']]),
        space0.all_by_keys(0, [['ilya'], ['vasya']], limit: 1),
        space0.all_by_keys(0, [['ilya'], ['vasya']], limit: 1, offset: 1),
        space0.all_by_keys(2, 13),
        space0.all_by_keys(1, [['zimov','il@zi.bot']]),
      ] }
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
      results = fibrun { [
        space0.all_by_key(0, 'vasya'),
        space0.all_by_key(0, ['vasya']),
        space0.all_by_key(2, 13),
        space0.all_by_key(1, ['zimov','il@zi.bot']),
      ] }
      results[0].must_equal [vasya]
      results[1].must_equal [vasya]
      (results[2] - [ilya, fedor]).must_be_empty
      results[3].must_equal [ilya]
    end

    it "should be able to first_by_key" do
      results = fibrun { [
        space0.first_by_key(0, 'vasya'),
        space0.first_by_key(0, ['ilya']),
        space0.first_by_key(2, 13),
        space0.first_by_key(1, ['petrov','eb@lo.com']),
      ] }
      results[0].must_equal vasya
      results[1].must_equal ilya
      [ilya, fedor].must_include results[2]
      results[3].must_equal vasya
    end

    it "should be able to by_pk" do
      results = fibrun { [
        space0.by_pk('vasya'),
        space0.by_pk(['ilya']),
        space2.by_pk(['hi zo', 'ho zo']),
      ] }
      results[0].must_equal vasya
      results[1].must_equal ilya
      results[2].must_equal ['hi zo', 'ho zo', 1]
    end
    
    it "should raise on not matched pk" do
      proc {
        space0.by_pk(['il','ya'])
      }.must_raise EM::Tarantool::ValueError
    end

    it "should fetch longer records" do
      results = fibrun { [
        space2.by_pk(['hi zo', 'pidas']),
        space1.by_pk(2),
      ]}
      results[0].must_equal ['hi zo', 'pidas', 1, 3, 5]
      results[1].must_equal [2, 'medium', 6, 'common', 7]
    end

    it "should be able to insert" do
      asdf = ['asdf', 'asdf', 'asdf', 4, 5]
      qwer = ['qwer', 'qwer', 'qwer', 4, 20, 19]
      zxcv = [4, 'zxcv', 7, 'zxcv', 8]
      xcvb = [5, 'xcvb', 7, 'xcvb', 8]
      results = fibrun {[
        space0.insert(asdf),
        space0.insert(qwer, return_tuple: true),
        space1.insert(zxcv),
        space1.by_pk(4),
        space1.insert(xcvb, return_tuple: true),
      ]}
      results[0].must_equal 1
      results[1].must_equal qwer
      results[2].must_equal 1
      results[3].must_equal zxcv
      results[4].must_equal xcvb
    end

    it "should be able to update" do
      results = fibrun {[
        space0.update('vasya', {1 => 'holodov', 3 => [:+, 2]}, return_tuple: true),
        space0.update('ilya', {[2, :set] => 'we@al.hero', 3 => [:&, 7]}, return_tuple: true),
        space1.update(2, [[2, :^, 3], [4, :|, 20]]),
        space1.by_pk(2),
        space1.update(1, [1, :splice, 2, 2, 'nd'], return_tuple: true),
        space2.update(['hi zo', 'pidas'], [[2, :delete], [3, :delete]], return_tuple: true),
        space2.update(['coma', 'peredoma'], [2, :insert, 1]),
        space2.all_by_key(1, 1),
      ]}
      results[0].must_equal ['vasya', 'holodov', 'eb@lo.com', 7]
      results[1].must_equal ['ilya', 'zimov', 'we@al.hero', 5]
      results[2].must_equal 1
      results[3].must_equal [2, 'medium', 5, 'common', 23]
      results[4].must_equal [1, 'condon', 4]
    end

    it "should be able to delete" do
      results = fibrun {[
        space0.delete('vasya', return_tuple: true),
        space1.delete([1], return_tuple: true),
        space2.delete(['hi zo', 'pidas'], return_tuple: true),
      ]}
      results[0].must_equal vasya
      results[1].must_equal [1, 'common', 4]
      results[2].must_equal ['hi zo', 'pidas', 1, 3, 5]
    end

    it "should be able to choose index by field numbers" do
      results = fibrun {[
        space0.first_by_key([0], 'vasya'),
        space0.first_by_key([1,2], ['zimov', 'il@zi.bot']),
        space0.all_by_key([3], 13),
        space2.first_by_key([1,0], ['peredoma', 'coma']),
      ]}
      results[0].must_equal vasya
      results[1].must_equal ilya
      (results[2] - [ilya, fedor]).must_be_empty
      ([ilya, fedor] - results[2]).must_be_empty
      results[3].must_equal ['coma', 'peredoma', 2]
    end
  end
end
