require File.expand_path('../helper.rb', __FILE__)

describe 'Tarantool::CallbackDB::SpaceArray' do
  before { clear_db }

  let(:tarantool) { Tarantool.new(TCONFIG.merge(type: :em_callback)) }
  let(:clear_space) { tarantool.space_array(0) }

  describe "without description" do
    let(:vasya){ %W{vasya petrov eb@lo.com \x05\x00\x00\x00} }
    let(:ilya) { %W{ilya  zimov  il@zi.bot \x0D\x00\x00\x00} }
    let(:fedor){ %W{fedor kuklin ku@kl.in  \x0D\x00\x00\x00} }
    it "should be selectable" do
      results = []
      emrun(8) {
        clear_space.select(0, 'vasya'){|res| results[0] = res; emstop}
        clear_space.select(0, ['vasya']){|res| results[1] = res; emstop}
        clear_space.select(0, ['vasya', 'ilya']){|res|
          results[2] = res; emstop
        }
        clear_space.select(0, [['vasya'], ['ilya']]){|res|
          results[3] = res; emstop
        }
        clear_space.select(0, [['ilya'], ['vasya']]){|res|
          results[4] = res; emstop
        }
        clear_space.select(0, [['ilya'], ['vasya']], 0, 1){|res|
          results[5] = res; emstop
        }
        clear_space.select(0, [['ilya'], ['vasya']], 1, 1){|res|
          results[6] = res; emstop
        }
        clear_space.select(2, "\x0D\x00\x00\x00", 0, 2){|res|
          results[7] = res; emstop
        }
        clear_space.select(2, 13, 0, 2){|res|
          results[8] = res; emstop
        }
        clear_space.select(1, [['zimov','il@zi.bot']]){|res|
          results[9] = res; emstop
        }
      }
      results[0].must_equal [vasya]
      results[1].must_equal [vasya]
      results[2].must_equal [vasya, ilya]
      results[3].must_equal [vasya, ilya]
      results[4].must_equal [ilya, vasya]
      results[5].must_equal [ilya]
      results[6].must_equal [vasya]
      results[7].sort.must_equal [fedor, ilya]
      results[8].sort.must_equal [fedor, ilya]
      results[9].must_equal [ilya]
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
        clear_space.all_by_keys(2, 13){|res|
          results[8] = res; emstop
        }
        clear_space.all_by_keys(1, [['zimov','il@zi.bot']]){|res|
          results[9] = res; emstop
        }
      }
      results[0].must_equal [vasya]
      results[1].must_equal [vasya]
      results[2].must_equal [vasya, ilya]
      results[3].must_equal [vasya, ilya]
      results[4].must_equal [ilya, vasya]
      results[5].must_equal [ilya]
      results[6].must_equal [vasya]
      results[7].sort.must_equal [fedor, ilya]
      results[8].sort.must_equal [fedor, ilya]
      results[9].must_equal [ilya]
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
        tarantool.space_array(2).by_pk(['hi zo', 'ho zo']){|res|
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

    it "should be able to update" do
      results = []
      emrun(3) {
        clear_space.update('vasya', {
          1 => 'korkov',
          2 => ['set', 'no@mo.re'],
          3 => ['add', 1]}, return_tuple: true){|res|
          results[0] = res; emstop
        }
        clear_space.update('ilya', {'&3' => 12}, return_tuple: true){|res|
          results[1] = res; emstop
        }
        clear_space.update('fedor', {'|3' => 3}, return_tuple: true){|res|
          results[2] = res; emstop
        }
      }
      results[0].must_equal %W{vasya korkov no@mo.re \x06\x00\x00\x00}
      results[1].must_equal %W{ilya zimov il@zi.bot \x0C\x00\x00\x00}
      results[2].must_equal %W{fedor kuklin ku@kl.in \x0F\x00\x00\x00}
    end
  end

end
