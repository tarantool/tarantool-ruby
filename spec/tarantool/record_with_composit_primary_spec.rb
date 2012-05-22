# -*- coding: utf-8 -*-
require 'spec_helper'
require 'tarantool/record'
require 'yajl'
require 'tarantool/serializers/bson'
describe Tarantool::Record do
  include Helpers::Truncate
  before do
    Tarantool.singleton_space.space_no = 2
    @primary_key_size = 2
  end
  let(:address_class) do
    Class.new(Tarantool::Record) do
      def self.name # For naming
        "Adress"
      end
      self.space_no = 2
      field :city, :string
      field :street, :string
      index :city, :street, primary: true
    end
  end

  describe "composite primary key" do
    
    before do
      address_class.create city: "Moscow", street: "Leningradskii"
      address_class.create city: "Moscow", street: "Mohovaya"
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