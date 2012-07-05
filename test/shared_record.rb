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
end
