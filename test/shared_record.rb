require File.expand_path('../helper.rb', __FILE__)
require 'yajl'
require 'tarantool/serializers/bson'

shared_examples_for :record do
  DB = Tarantool.new(TCONFIG.merge(type: :block))
  before{ truncate }

  let(:user_class) do
    Class.new(base_class) do
      set_tarantool DB
      set_space_no 0

      def self.name # For naming
        "User"
      end

      field :login, :string
      field :name, :string
      field :email, :string
      field :apples_count, :integer, default: 0
      index :name, :email
    end
  end

  let(:user) { user_class.new }

  it "should set and get attributes" do
    user.name = 'Andrew'
    user.name.must_equal 'Andrew'
  end

  describe "inheritance" do
    let(:author_class) do
      Class.new(user_class) do
        field :best_book, Integer
      end
    end
    let(:artist_class) do
      Class.new(user_class) do
        field :imdb_id, Integer
      end
    end

    describe "Artist from User" do
      it "should has only itself field" do
        artist_class.fields.keys.must_equal [:login, :name, :email, :apples_count, :imdb_id]
      end
    end

    it "should has same space no as parent" do
      user_class.space_no = 1
      artist_class.space_no.must_equal 1
    end

    it "should has different space no to parent if setted" do
      artist_class.space_no = 1
      user_class.space_no.must_equal 0
      artist_class.space_no.must_equal 1
    end
  end

  describe "save" do
    it "should save and select record" do
      u = user_class.new login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru'
      u.save
      u = user_class.find 'prepor'
      u.id.must_equal 'prepor'
      u.email.must_equal 'ceo@prepor.ru'
      u.name.must_equal 'Andrew'
      u.apples_count.must_equal 0
    end

    it "should update dirty attributes" do
      u = user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru'
      u.name = 'Petr'
      u.save
      u = user_class.find 'prepor'
      u.email.must_equal 'ceo@prepor.ru'
      u.name.must_equal 'Petr'
    end

    describe "with nils" do
      before do
        user_class.field :score, :integer
        user_class.field :info, Tarantool::Serializers::BSON #:bson
      end
      it "should work properly with nils values" do
        u = user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru', apples_count: 1
        u.info.must_be_nil
        u.score.must_be_nil
        u.reload
        u.info.must_be_nil
        u.score.must_be_nil
        u.info = {'bio' => 'hi!'}
        u.score = 1
        u.save
        u.reload
        u.info.must_equal({ 'bio' => 'hi!' })
        u.score.must_equal 1
      end
    end
  end

  describe "reload" do
    it "should reload current record" do
      u = user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru'
      u.name = 'Petr'
      u.reload.name.must_equal 'Andrew'
    end
  end

  describe "update" do
    let(:user) { user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru' }
    it "should update fields" do
      user.update(name: "Andrey", email: [":", 0, 3, "prepor"], apples_count: [:+, 7])
      user.name.must_equal "Andrey"
      user.email.must_equal "prepor@prepor.ru"
      user.apples_count.must_equal 7

      user.update(apples_count: [:^, 12])
      user.apples_count.must_equal 11
    end

    describe "increment" do
      it "should increment apples count by 1" do
        user.increment :apples_count
        user.apples_count.must_equal 1
      end

      it "should increment apples count by 3" do
        user.increment :apples_count, 3
        user.apples_count.must_equal 3
      end
    end
  end

  describe "destroy" do
    it "should destroy record" do
      u = user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru'
      u.destroy
      u.reload.must_equal false
    end
  end

  describe "select" do
    describe "by name Andrew" do
      let(:select) { user_class.where(name: 'Andrew') }
      before do
        user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru'
        user_class.create login: 'petro', name: 'Petr', email: 'petro@gmail.com'
        user_class.create login: 'ruden', name: 'Andrew', email: 'rudenkoco@gmail.com'
      end
      it "should select all records with name == 'Andrew'" do
        select.all.map(&:login).must_equal ['prepor', 'ruden']
      end

      it "should select first record with name == 'Andrew'" do
        select.first.login.must_equal 'prepor'
      end

      it "should select 1 record by name and email" do
        user_class.where(name: 'Andrew', email: 'rudenkoco@gmail.com').map(&:login).must_equal ['ruden']
      end

      it "should select 2 record by name and email" do
        user_class.where(name: ['Andrew', 'Andrew'], email: ['ceo@prepor.ru', 'rudenkoco@gmail.com']).map(&:login).must_equal ['prepor', 'ruden']
      end

      it "should select 3 record by names" do
        user_class.where(name: ['Andrew', 'Petr']).map(&:login).must_equal ['prepor', 'ruden', 'petro']
      end

      it "should raise an error for wrong index" do
        proc{ user_class.where(namee: "Andrew").all }.must_raise Tarantool::ValueError
      end

      describe "with limit 1" do
        let(:select_limit) { select.limit(1) }
        it "should select first record with name == 'Andrew'" do
          select_limit.map(&:login).must_equal ['prepor']
        end

        describe "with offset 1" do
          let(:select_offset) { select_limit.offset(1) }
          it "should select last record with name == 'Andrew'" do
            select_offset.map(&:login).must_equal ['ruden']
          end
        end
      end
    end

    describe "==" do
      before do
        user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru'
        user_class.create login: 'petro', name: 'Petr', email: 'petro@gmail.com'
      end

      it "should return equality as true" do
        u1 = user_class.where(login: "prepor").first
        u2 = user_class.where(login: "prepor").first
        (u1 == u2).must_equal true
      end

      it "should not return equality as true" do
        u1 = user_class.where(login: "prepor")
        u2 = user_class.where(login: "petro")
        (u1 == u2).must_equal false
      end
    end


    describe "call" do
      let(:res) { user_class.select.call('box.select_range', 0, 2)}

      it "should select 2 records and return UserClass instances" do
        user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru'
        user_class.create login: 'petro', name: 'Petr', email: 'petro@gmail.com'
        user_class.create login: 'ruden', name: 'Andrew', email: 'rudenkoco@gmail.com'
        res.size.must_equal 2
        res.any? { |v| v.is_a? user_class }
      end
    end
  end

  describe "light api" do
    let(:prepor){ {login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru'} }
    let(:petro){ {login: 'petro', name: 'Petr', email: 'petro@gmail.com'} }
    let(:ruden){ {login: 'ruden', name: 'Andrew', email: 'rudenkoco@gmail.com'} }
    before do
      user_class.create prepor
      user_class.create petro
      user_class.create ruden
    end

    it "should search by_pk" do
      user_class.by_pk('prepor').email.must_equal 'ceo@prepor.ru'
      user_class.by_pk(['petro']).email.must_equal 'petro@gmail.com'
      user_class.by_pk(login: 'ruden').email.must_equal 'rudenkoco@gmail.com'
    end

    it "should search by_pks" do
      user_class.by_pks(['prepor', ['petro'], {login: 'ruden'}]).
        map(&:email).must_equal %w{ceo@prepor.ru petro@gmail.com rudenkoco@gmail.com}
    end

    it "should find" do
      user_class.find('prepor').email.must_equal 'ceo@prepor.ru'
      user_class.find(['petro']).email.must_equal 'petro@gmail.com'
      user_class.find(login: 'ruden').email.must_equal 'rudenkoco@gmail.com'
      user_class.find('prepor', ['petro'], {login: 'ruden'}).
        map(&:email).must_equal %w{ceo@prepor.ru petro@gmail.com rudenkoco@gmail.com}
    end

    it "should find first" do
      user_class.first(name: 'Petr', email: 'petro@gmail.com').login.must_equal 'petro'
      ['prepor', 'ruden'].must_include user_class.first(name: 'Andrew').login
    end

    it "should find all" do
      user_class.select(name: 'Andrew').map(&:login).sort.must_equal %w{prepor ruden}

      limited = user_class.select({name: 'Andrew'}, limit: 1).map(&:login)
      limited.size.must_equal 1
      limited.must_equal limited & %w{prepor ruden}

      offsetted = user_class.all({name: 'Andrew'}, offset: 1).map(&:login)
      offsetted.size.must_equal 1
      offsetted.must_equal offsetted & %w{prepor ruden}
      limited.wont_equal offsetted
      (limited+offsetted).sort.must_equal %w{prepor ruden}

      user_class.all(name: 'Andrew', email: 'ceo@prepor.ru').map(&:login).must_equal %w{prepor}
    end

    let(:zuma){ {login: 'zuma', name: 'Zuma', email: 'a@b.c', apples_count: 0} }
    let(:vova){ {login: 'vova', name: 'Vova', email: 'a@b.c'} }
    let(:_lila){ {login: 'lila', name: 'lila', email: 'l@i.la', apples_count: 1} }

    it "should insert" do
      user_class.insert(zuma).must_equal 1
      proc {
        user_class.insert(zuma)
      }.must_raise Tarantool::TupleExists
      user = user_class.insert(_lila, true)
      user.must_be_instance_of user_class
      user.attributes.must_equal _lila
      user.attributes.wont_be_same_as _lila
    end

    it "should replace" do
      proc {
        user_class.replace(vova)
      }.must_raise Tarantool::TupleDoesntExists

      user_class.replace(ruden.merge(apples_count: 10)).must_equal 1
      user_class.by_pk('ruden').apples_count.must_equal 10

      user_class.replace(petro.merge(apples_count: 11), true).
        attributes.must_equal petro.merge(apples_count: 11)
    end

    it "should update" do
      user_class.update('ruden', {apples_count: [:+, 2]})
      user_class.by_pk('ruden').apples_count.must_equal 2

      user_class.update('prepor', {apples_count: [:+, 2]}, true).
        attributes.must_equal prepor.merge(apples_count: 2)

      user_class.update('fdsaf', {name: 'no'}).must_equal 0
      user_class.update('fdsaf', {name: 'no'}, true).must_be_nil
    end

    it "should delete" do
      user_class.delete(login: 'ruden').must_equal 1
      user_class.by_pk('ruden').must_be_nil
    end

    it "should invoke" do
      user_class.by_pk('ruden').attributes.must_equal ruden.merge(apples_count: 0)
      user_class.invoke('box.delete', 'ruden').must_equal 1
      user_class.by_pk('ruden').must_be_nil

      user_class.by_pk('petro').attributes.must_equal petro.merge(apples_count: 0)
      user_class.invoke('box.delete', user_class.space_no, 'petro', space_no: nil).must_equal 1
      user_class.by_pk('petro').must_be_nil
    end

    it "should call" do
      user_class.by_pk('ruden').attributes.must_equal ruden.merge(apples_count: 0)
      user_class.call('box.delete', 'ruden')[0].attributes.must_equal ruden.merge(apples_count: 0)
      user_class.by_pk('ruden').must_be_nil

      user_class.by_pk('petro').attributes.must_equal petro.merge(apples_count: 0)
      user_class.call('box.delete', user_class.space_no, 'petro', space_no: nil)[0].
        attributes.must_equal petro.merge(apples_count: 0)
      user_class.by_pk('petro').must_be_nil
    end

    it "should call with custom returns" do
      v = user_class.call('box.delete', 'ruden', returns: [:str, :str, :str, :int])
      v.must_equal [ruden.values + [0]]
      v = user_class.call('box.delete', 'prepor', returns: {a: :str, b: :str, c: :str, d: :int})
      v.must_equal [{a: 'prepor', b: 'Andrew', c: 'ceo@prepor.ru', d: 0}]
    end

    describe "auto_space" do
      let(:auto_space){ user_class.auto_space }
      it "should return records on search" do
        p = auto_space.by_pk('prepor')
        p.must_be_instance_of user_class
        p.attributes.must_equal prepor.merge(apples_count: 0)

        p = auto_space.all_by_pks(['petro', {login: 'prepor'}])
        p[0].must_be_instance_of user_class
        p[1].must_be_instance_of user_class
        p.map(&:email).sort.must_equal %w[ceo@prepor.ru petro@gmail.com]

        p = auto_space.select({name: 'Andrew', email: 'rudenkoco@gmail.com'}, 0, 1)
        p[0].must_be_instance_of user_class
        p[0].attributes.must_equal ruden.merge(apples_count: 0)

        p = auto_space.call('box.select_range', [0, 1000])
        p.each{|o| o.must_be_instance_of user_class}
        p.map(&:login).sort.must_equal %w{petro prepor ruden}

        p = auto_space.insert({name: 'a', login: 'b', email: 'r', apples_count: 100}, return_tuple: true)
        p.must_be_instance_of user_class
        p.name.must_equal 'a'
        p.login.must_equal 'b'
        p.email.must_equal 'r'
        p.apples_count.must_equal 100
      end
    end
  end

  describe "fields serializers" do
    before do
      user_class.field :info, :bson
    end

    it "should serialise and deserialize info field" do
      info = { 'bio' => "hi!", 'age' => 23, 'hobbies' => ['mufa', 'tuka'] }
      u = user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru', info: info
      u.info['hobbies'].must_equal ['mufa', 'tuka']
      u.reload
      u.info['hobbies'].must_equal ['mufa', 'tuka']
      u = user_class.find u.login
      u.info['hobbies'].must_equal ['mufa', 'tuka']
    end
  end
end
