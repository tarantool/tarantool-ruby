# -*- coding: utf-8 -*-
require 'spec_helper'
require 'tarantool/record'
require 'yajl'
require 'tarantool/serializers/bson'
describe Tarantool::Record do
  include Helpers::Truncate

  describe "primary key" do
    def space
      @space ||= DB.space 2
    end

    before do
      @primary_key_size = 2
    end

    let(:address_class) do
      Class.new(Tarantool::Record) do
        set_tarantool DB
        set_space_no 2

        def self.name # For naming
          "Adress"
        end

        field :city, :string
        field :street, :string
        field :index, :integer
        field :name, :string
        field :citizens, :integer, default: 1
        index :city, :street, primary: true
        index :index
      end
    end

    describe "composite primary key" do
      
      before do
        address_class.create city: "Moscow", street: "Leningradskii", index: 123, name: "Pedro"
        address_class.create city: "Moscow", street: "Mohovaya", index: 123, name: "Pedro"
      end

      it "should return objects by second index" do
        a1 = address_class.where(index: 123)
        a1.all.size.must_equal 2
      end

      it "should work with increment" do
        a1 = address_class.where(city: "Moscow", street: "Leningradskii").first
        citizens = a1.citizens
        a1.increment :citizens
        a1.reload.citizens.must_equal(citizens+1)
      end

      it "should return all objects" do
        a1 = address_class.where city: "Moscow"
        a1.all.size.must_equal 2
        a1.all.map(&:street).must_equal ["Leningradskii", "Mohovaya"]
      end

      it "should return right object" do
        a1 = address_class.where city: "Moscow", street: "Leningradskii"
        a1.first.street.must_equal "Leningradskii"
      end

      it "should destroy object" do
        a1 = address_class.where city: "Moscow", street: "Leningradskii"
        a1.first.destroy
      end

      it "should return id" do
        a1 = address_class.where city: "Moscow", street: "Leningradskii"
        a1.first.id.must_equal ["Moscow", "Leningradskii"]
      end

    end
  end
end