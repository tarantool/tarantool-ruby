# -*- coding: utf-8 -*-
require 'spec_helper'
require 'tarantool/record'
require 'yajl'
require 'tarantool/serializers/bson'
describe Tarantool::Record do
  include Helpers::Truncate

  def space
    @space ||= DB.space 0
  end

  let(:user_class) do
    Class.new(Tarantool::Record) do
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

  describe "detect_index_no" do
    let(:select) { user_class.select }
    it "should return 0 for :login" do
      select.detect_index_no([:login]).must_equal 0
    end
    it "should return 1 for :name" do
      select.detect_index_no([:name]).must_equal 1
    end
    it "should return 1 for :name, :email" do
      select.detect_index_no([:name, :email]).must_equal 1
    end
    it "should return nil for :email" do
      select.detect_index_no([:email]).must_be_nil
    end
    it "should return nil for :login, :name" do
      select.detect_index_no([:login, :name]).must_be_nil
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
        user_class.field :info, :bson
      end
      it "should work properly with nils values" do
        u = user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru', apples_count: nil
        u.info.must_be_nil
        u.apples_count.must_be_nil
        u.reload
        u.info.must_be_nil
        u.apples_count.must_be_nil
        u.info = {'bio' => 'hi!'}
        u.apples_count = 1
        u.save
        u.reload
        u.info.must_equal({ 'bio' => 'hi!' })
        u.apples_count.must_equal 1
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

  describe "increment" do
    before { @truncate_fields = 2 }
    let(:user) { user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru' }
    it "should increment apples count by 1" do
      user.increment :apples_count
      user.reload.apples_count.must_equal 1
    end

    it "should increment apples count by 3" do
      user.increment :apples_count, 3
      user.reload.apples_count.must_equal 3
    end
  end

  describe "destroy" do
    it "should destroy record" do
      u = user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru'
      u.destroy
      u.reload.must_equal false
    end
  end

  describe "validations" do
    describe "with validator on login size" do
      before do
        user_class.validates_length_of(:login, minimum: 3)
      end
      it "should invalidate all records with login less then 3 chars" do
        u = user_class.new login: 'pr', name: 'Andrew', email: 'ceo@prepor.ru'
        u.save.must_equal false
        u.valid?.must_equal false
        u.errors.size.must_equal 1
        u.login = 'prepor'
        u.save.must_equal true
        u.valid?.must_equal true
        u.errors.size.must_equal 0
      end
    end
  end

  describe "callbacks" do
    let(:u) { user_class.new login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru '}
    it "should run before / after create callbackss in right places" do
      user_class.before_create :action_before_create
      user_class.after_create :action_after_create      
      mock(u).action_before_create { u.new_record?.must_equal true }
      mock(u).action_after_create { u.new_record?.must_equal false }
      u.save
    end

    describe "initialize" do
      it "should run after_initialize after any initialization" do
        user_class.after_initialize :action_after_initialize
        any_instance_of(user_class) do |u|
          mock(u).action_after_initialize.twice
        end
        u.save
        user_class.find u.login
      end

      it "should not run after_initialize after reload" do        
        user_class.after_initialize :action_after_initialize
        any_instance_of(user_class) do |u|
          mock(u).action_after_initialize.once
        end
        u.save
        u.reload
      end

      it "should properly save record inside after_initialize" do
        user_class.after_initialize do |u|
          u.save
        end
        u
        user_class.find u.login
      end
    end
  end

  describe "serialization" do
    it "should support AM serialization API" do
      h = { login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru' }
      u = user_class.create h
      u.as_json.must_equal({ 'user' => h.merge(apples_count: 0) })
    end

    describe "fields serilizers" do
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
        proc{ user_class.where(namee: "Andrew").all }.must_raise Tarantool::UndefinedIndex
      end

      describe "with limit 1" do
        let(:select) { super().limit(1) }
        it "should select first record with name == 'Andrew'" do
          select.map(&:login).must_equal ['prepor']
        end

        describe "with offset 1" do
          let(:select) { super().offset(1) }
          it "should select last record with name == 'Andrew'" do
            select.map(&:login).must_equal ['ruden']
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
      let(:res) { user.class.select.call('box.select_range', 0, 0, 2)}
      before do
        user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru'
        user_class.create login: 'petro', name: 'Petr', email: 'petro@gmail.com'
        user_class.create login: 'ruden', name: 'Andrew', email: 'rudenkoco@gmail.com'
      end

      it "should select 2 records and return UserClass instances" do
        res.size.must_equal 2
        res.any? { |v| v.is_a? user_class }
      end
    end
  end

end