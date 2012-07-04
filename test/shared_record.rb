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
        user_class.field :info, :bson
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

end
