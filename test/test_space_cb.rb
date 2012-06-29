require File.expand_path('../helper.rb', __FILE__)

describe EM::Tarantool::SpaceCB do
  before { clear_db }

  let(:tarantool) { EM::Tarantool.new(TCONFIG[:host], TCONFIG[:port]) }
  let(:clear_space) { tarantool.space_cb(0) }
  let(:space0) { tarantool.space_cb(0, SPACE0[:types], pk: SPACE0[:pk], indexes: SPACE0[:indexes])}
  let(:space1) { tarantool.space_cb(1, SPACE1[:types], pk: SPACE1[:pk], indexes: SPACE1[:indexes])}
  let(:space2) { tarantool.space_cb(2, SPACE2[:types], pk: SPACE2[:pk], indexes: SPACE2[:indexes])}

  it "should be got from tarantool" do
    space0 = tarantool.space_cb(0)
    space0.must_be_kind_of EM::Tarantool::SpaceCB
  end

  describe "without description" do
    let(:vasya){ %W{vasya petrov eb@lo.com \x05\x00\x00\x00} }
    let(:ilya) { %W{ilya  zimov  il@zi.bot \x0D\x00\x00\x00} }
    let(:fedor){ %W{fedor kuklin ku@kl.in  \x0D\x00\x00\x00} }
    it "should be selectable" do
      results = []
      emrun(8) {
        clear_space.select(0, 0, -1, 'vasya'){|res| results[0] = res; emstop}
        clear_space.select(0, 0, -1, ['vasya']){|res| results[1] = res; emstop}
        clear_space.select(0, 0, -1, ['vasya', 'ilya']){|res|
          results[2] = res; emstop
        }
        clear_space.select(0, 0, -1, [['vasya'], ['ilya']]){|res|
          results[3] = res; emstop
        }
        clear_space.select(0, 0, -1, [['ilya'], ['vasya']]){|res|
          results[4] = res; emstop
        }
        clear_space.select(0, 0, 1, [['ilya'], ['vasya']]){|res|
          results[5] = res; emstop
        }
        clear_space.select(0, 1, 1, [['ilya'], ['vasya']]){|res|
          results[6] = res; emstop
        }
        clear_space.select(2, 0, 2, "\x0D\x00\x00\x00"){|res|
          results[7] = res; emstop
        }
        clear_space.select(1, 0, -1, [['zimov','il@zi.bot']]){|res|
          results[8] = res; emstop
        }
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
        clear_space.all_by_keys(0, 'vasya'){|res| results[0] = res; emstop}
        clear_space.all_by_keys(0, ['vasya']){|res| results[1] = res; emstop}
        clear_space.all_by_keys(0, ['vasya', 'ilya']){|res|
          results[2] = res; emstop
        }
        clear_space.all_by_keys(0, [['vasya'], ['ilya']]){|res|
          results[3] = res; emstop
        }
        clear_space.all_by_keys(0, [['ilya'], ['vasya']]){|res|
          results[4] = res; emstop
        }
        clear_space.all_by_keys(0, [['ilya'], ['vasya']], limit: 1){|res|
          results[5] = res; emstop
        }
        clear_space.all_by_keys(0, [['ilya'], ['vasya']], limit: 1, offset: 1){|res|
          results[6] = res; emstop
        }
        clear_space.all_by_keys(2, "\x0D\x00\x00\x00"){|res|
          results[7] = res; emstop
        }
        clear_space.all_by_keys(1, [['zimov','il@zi.bot']]){|res|
          results[8] = res; emstop
        }
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
        clear_space.all_by_key(0, 'vasya'){|res| results[0] = res; emstop}
        clear_space.all_by_key(0, ['vasya']){|res| results[1] = res; emstop}
        clear_space.all_by_key(2, "\x0D\x00\x00\x00"){|res|
          results[2] = res; emstop
        }
        clear_space.all_by_key(1, ['zimov','il@zi.bot']){|res|
          results[3] = res; emstop
        }
      }
      results[0].must_equal [vasya]
      results[1].must_equal [vasya]
      (results[2] - [ilya, fedor]).must_be_empty
      results[3].must_equal [ilya]
    end

    it "should be able to first_by_key" do
      results = []
      emrun(4) { 
        clear_space.first_by_key(0, 'vasya'){|res| results[0] = res; emstop}
        clear_space.first_by_key(0, ['ilya']){|res| results[1] = res; emstop}
        clear_space.first_by_key(2, "\x0D\x00\x00\x00"){|res|
          results[2] = res; emstop
        }
        clear_space.first_by_key(1, ['petrov','eb@lo.com']){|res|
          results[3] = res; emstop
        }
      }
      results[0].must_equal vasya
      results[1].must_equal ilya
      [ilya, fedor].must_include results[2]
      results[3].must_equal vasya
    end

    it "should be able to by_pk" do
      results = []
      emrun(3) { 
        clear_space.by_pk('vasya'){|res| results[0] = res; emstop}
        clear_space.by_pk(['ilya']){|res| results[1] = res; emstop}
        tarantool.space_cb(2).by_pk(['hi zo', 'ho zo']){|res|
          results[2] = res; emstop
        }
      }
      results[0].must_equal vasya
      results[1].must_equal ilya
      results[2].must_equal ['hi zo', 'ho zo', "\x01\x00\x00\x00"]
    end

    it "should be able to insert" do
      results = []
      emrun(2) {
        clear_space.insert(%w{asdf asdf asdf asdf asdf}){|res| results[0] = res; emstop}
        clear_space.insert(%w{qwer qwer qwer qwer qwer}, return_tuple: true){|res|
          results[1] = res; emstop
        }
      }
      results[0].must_equal 1
      results[1].must_equal %w{qwer qwer qwer qwer qwer}
    end
  end

  describe "with definition" do
    let(:vasya){ ['vasya', 'petrov', 'eb@lo.com', 5] }
    let(:ilya) { ['ilya', 'zimov', 'il@zi.bot', 13] }
    let(:fedor){ ['fedor', 'kuklin', 'ku@kl.in', 13] }
    it "should be selectable" do
      results = []
      emrun(8) { 
        space0.select(0, 0, -1, 'vasya'){|res| results[0] = res; emstop}
        space0.select(0, 0, -1, ['vasya']){|res| results[1] = res; emstop}
        space0.select(0, 0, -1, ['vasya', 'ilya']){|res|
          results[2] = res; emstop
        }
        space0.select(0, 0, -1, [['vasya'], ['ilya']]){|res|
          results[3] = res; emstop
        }
        space0.select(0, 0, -1, [['ilya'], ['vasya']]){|res|
          results[4] = res; emstop
        }
        space0.select(0, 0, 1, [['ilya'], ['vasya']]){|res|
          results[5] = res; emstop
        }
        space0.select(0, 1, 1, [['ilya'], ['vasya']]){|res|
          results[6] = res; emstop
        }
        space0.select(2, 0, 2, 13){|res|
          results[7] = res; emstop
        }
        space0.select(1, 0, -1, [['zimov','il@zi.bot']]){|res|
          results[8] = res; emstop
        }
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
        space0.all_by_keys(0, 'vasya'){|res| results[0] = res; emstop}
        space0.all_by_keys(0, ['vasya']){|res| results[1] = res; emstop}
        space0.all_by_keys(0, ['vasya', 'ilya']){|res|
          results[2] = res; emstop
        }
        space0.all_by_keys(0, [['vasya'], ['ilya']]){|res|
          results[3] = res; emstop
        }
        space0.all_by_keys(0, [['ilya'], ['vasya']]){|res|
          results[4] = res; emstop
        }
        space0.all_by_keys(0, [['ilya'], ['vasya']], limit: 1){|res|
          results[5] = res; emstop
        }
        space0.all_by_keys(0, [['ilya'], ['vasya']], limit: 1, offset: 1){|res|
          results[6] = res; emstop
        }
        space0.all_by_keys(2, 13){|res| results[7] = res; emstop }
        space0.all_by_keys(1, [['zimov','il@zi.bot']]){|res|
          results[8] = res; emstop
        }
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
        space0.all_by_key(0, 'vasya'){|res| results[0] = res; emstop}
        space0.all_by_key(0, ['vasya']){|res| results[1] = res; emstop}
        space0.all_by_key(2, 13){|res| results[2] = res; emstop }
        space0.all_by_key(1, ['zimov','il@zi.bot']){|res|
          results[3] = res; emstop
        }
      }
      results[0].must_equal [vasya]
      results[1].must_equal [vasya]
      (results[2] - [ilya, fedor]).must_be_empty
      results[3].must_equal [ilya]
    end

    it "should be able to first_by_key" do
      results = []
      emrun(4) { 
        space0.first_by_key(0, 'vasya'){|res| results[0] = res; emstop}
        space0.first_by_key(0, ['ilya']){|res| results[1] = res; emstop}
        space0.first_by_key(2, 13){|res| results[2] = res; emstop }
        space0.first_by_key(1, ['petrov','eb@lo.com']){|res|
          results[3] = res; emstop
        }
      }
      results[0].must_equal vasya
      results[1].must_equal ilya
      [ilya, fedor].must_include results[2]
      results[3].must_equal vasya
    end

    it "should be able to by_pk" do
      results = []
      emrun(3) { 
        space0.by_pk('vasya'){|res| results[0] = res; emstop}
        space0.by_pk(['ilya']){|res| results[1] = res; emstop}
        space2.by_pk(['hi zo', 'ho zo']){|res|
          results[2] = res; emstop
        }
      }
      results[0].must_equal vasya
      results[1].must_equal ilya
      results[2].must_equal ['hi zo', 'ho zo', 1]
    end
    
    it "should raise on not matched pk" do
      proc {
        emrun(1) {
          space0.by_pk(['il','ya']){|res| assert false; emstop }
        }
      }.must_raise EM::Tarantool::ValueError
    end

    it "should fetch longer records" do
      results = []
      emrun(2) {
        space2.by_pk(['hi zo', 'pidas']){|res| results[0] = res; emstop}
        space1.by_pk(2){|res| results[1] = res; emstop}
      }
      results[0].must_equal ['hi zo', 'pidas', 1, 3, 5]
      results[1].must_equal [2, 'medium', 6, 'common', 7]
    end

    it "should be able to insert" do
      results = []
      asdf = ['asdf', 'asdf', 'asdf', 4, 5]
      qwer = ['qwer', 'qwer', 'qwer', 4, 20, 19]
      zxcv = [4, 'zxcv', 7, 'zxcv', 8]
      emrun(3) {
        space0.insert(asdf){|res| results[0] = res; emstop}
        space0.insert(qwer, return_tuple: true){|res| results[1] = res; emstop }
        space1.insert(zxcv){|res|
          results[2] = res
          space1.by_pk(4){|res|
            results[3] = res
            emstop
          }
        }
      }
      results[0].must_equal 1
      results[1].must_equal qwer
      results[2].must_equal 1
      results[3].must_equal zxcv
    end

    it "should be able to update" do
      results = []
      emrun(6) {
        space0.update('vasya', {1 => 'holodov', 3 => [:+, 2]}, return_tuple: true){|res|
          results[0] = res; emstop
        }
        space0.update('ilya', {[2, :set] => 'we@al.hero', 3 => [:&, 7]}, return_tuple: true){|res|
          results[1] = res; emstop
        }
        space1.update(2, [[2, :^, 3], [4, :|, 20]]){|res|
          results[2] = res
          space1.by_pk(2){|res|
            results[3] = res; emstop
          }
        }
        space1.update(1, [1, :splice, 2, 2, 'nd'], return_tuple: true){|res|
          results[4] = res
          emstop
        }
        space2.update(['hi zo', 'pidas'], [[2, :delete], [3, :delete]], return_tuple: true){|res|
          results[5] = res
          emstop
        }
        space2.update(['coma', 'peredoma'], [2, :insert, 1]){|res|
          results[6] = res
          space2.all_by_key(1, 1){|res|
            results[7] = res
            emstop
          }
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
        space0.delete('vasya', return_tuple: true){|res|
          results[0] = res; emstop
        }
        space1.delete([1], return_tuple: true){|res|
          results[1] = res; emstop
        }
        space2.delete(['hi zo', 'pidas'], return_tuple: true){|res|
          results[2] = res; emstop
        }
      }
      results[0].must_equal ['vasya', 'petrov', 'eb@lo.com', 5]
      results[1].must_equal [1, 'common', 4]
      results[2].must_equal ['hi zo', 'pidas', 1, 3, 5]
    end
  end
end
