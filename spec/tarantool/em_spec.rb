# -*- coding: utf-8 -*-
require 'spec_helper'
require "em-synchrony"
describe "Tarantool with EM" do
  def space
    @space ||= Tarantool.new(TARANTOOL_CONFIG.merge(type: :em)).space 1
  end

  describe "insert, select and delete" do
    it "should insert tuple and return it" do
      EM.synchrony do
        space.insert 100, 'привет', return_tuple: true
        res = space.select 100
        int, string = res.tuple
        int.to_i.must_equal 100
        string.to_s.must_equal 'привет'
        space.delete 100
        EM.stop
      end
    end
  end
end