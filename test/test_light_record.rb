# -*- coding: utf-8 -*-
require File.expand_path('../shared_record', __FILE__)
require 'tarantool/light_record'

describe 'Tarantool::LightRecord' do
  let(:base_class){ Tarantool::LightRecord }
  it_behaves_like :record

  describe "increment" do
    let(:user) { user_class.create login: 'prepor', name: 'Andrew', email: 'ceo@prepor.ru' }
    it "should increment apples count by 1" do
      user.increment :apples_count
      user.apples_count.must_equal 1
    end

    it "should increment apples count by 3" do
      user.increment :apples_count, 3
      user.apples_count.must_equal 3
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
      proc {
        # missed field apples_count, which is referenced in index
        user_class.insert(vova)
      }.must_raise Tarantool::IllegalParams
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
  end
end
