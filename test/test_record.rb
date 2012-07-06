# -*- coding: utf-8 -*-
require File.expand_path('../shared_record', __FILE__)
require 'tarantool/record'

describe 'Tarantool::Record' do
  let(:base_class){ Tarantool::Record }
  it_behaves_like :record

  describe "increment" do
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
  end
end
