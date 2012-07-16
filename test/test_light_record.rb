# -*- coding: utf-8 -*-
require File.expand_path('../shared_record', __FILE__)
require 'tarantool/light_record'

describe 'Tarantool::LightRecord' do
  before { TConf.run(:master1) }

  let(:base_class){ Tarantool::LightRecord }
  it_behaves_like :record

  describe "update" do
    let(:user) { user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru' }
    it "should reload fields event not reffered in operations" do
      user.name = "Petr"
      user.update(email: "prepor@ceo.ru")
      user.name.must_equal "Andrew"
      user.email.must_equal "prepor@ceo.ru"

      fetched = user_class.by_pk('prepor')
      fetched.name.must_equal "Andrew"
      fetched.email.must_equal "prepor@ceo.ru"
    end
  end

  describe "callbacks" do
    let(:u) { user_class.new login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru '}
    it "should run after_init method on creation" do
      any_instance_of(user_class) do |u|
        mock(u).after_init.once
      end
      u.save
    end

    it "should run after_init method on creation and fetching" do
      any_instance_of(user_class) do |u|
        mock(u).after_init.twice
      end
      u.save
      user_class.find u.login
    end

    it "should not run after_init on reload" do
      any_instance_of(user_class) do |u|
        mock(u).after_init.once
      end
      u.save
      u.reload
    end
  end
end
