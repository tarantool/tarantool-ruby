# -*- coding: utf-8 -*-
require 'spec_helper'

describe Tarantool::Request do
  before do
    Tarantool.singleton_space.space_no = 1
  end
  describe "pack method" do
    describe "for field" do
      it "should pack integer as 32 bit integer" do
        size, value = Tarantool::Request.pack_field(5).unpack('wL')
        value.must_equal 5
      end

      it "should pack string as arbitrary binary string" do
        size, value = Tarantool::Request.pack_field("привет").unpack('wa*')
        value.force_encoding('utf-8').must_equal 'привет'
      end

      it "should raise ArgumentError for other types" do
        lambda { Tarantool::Request.pack_field(:foo) }.must_raise Tarantool::ArgumentError
      end
    end

    describe "for tuple" do
      it "should pack to fields with fields count" do
        field1, field2 = 1, "привет"
        expect = [2, 4, field1, field2.bytesize, field2].pack('LwLwa*')
        Tarantool::Request.pack_tuple(field1, field2).must_equal expect
      end
    end
  end

  describe "instance" do
    let(:request) { Tarantool::Requests::Insert.new Tarantool.space }

    it "should make packet with right request type, body size and next request id + body" do
      body = 'hi'
      expect = [Tarantool::Requests::REQUEST_TYPES[:insert], body.bytesize, request.request_id].pack('LLL') + body
      request.make_packet(body).must_equal expect
    end

  end

  describe "requests" do
    include Helpers::Truncate
    describe "insert and select" do
      it "should insert tuple and return it" do
        Tarantool.insert 100, 'привет', return_tuple: true
        res = Tarantool.select 100
        int, string = res.tuple
        int.to_i.must_equal 100
        string.to_s.must_equal 'привет'
      end

      describe "with equal ids" do
        it "should raise error" do
          Tarantool.insert 100, 'lala'
          lambda { Tarantool.insert 100, 'yo' }.must_raise(Tarantool::BadReturnCode)
        end
      end

      # it "should raise timeout_exception on select" do
      #   Tarantool.call proc_name: 'box.fiber.sleep', args: ['4']
      # end
    end

    describe "select" do
      it "should select multiple tuples" do
        Tarantool.insert 100, 'привет'
        Tarantool.insert 101, 'hi'
        res = Tarantool.select 100, 101
        res.tuples.map { |v| v.last.to_s }.must_equal ['привет', 'hi']
      end
    end

    describe "call" do
      it "should call lua proc" do
        res = Tarantool.call proc_name: 'box.pack', args: ['i', '100'], return_tuple: true
        res.tuple[0].to_i.must_equal 100
      end

      it "should return batches via select_range" do
        Tarantool.insert 100, 'привет'
        Tarantool.insert 101, 'hi'
        res = Tarantool.call proc_name: 'box.select_range', args: ['1', '0', '100'], return_tuple: true
        res.tuples.size.must_equal 2
      end
    end

    describe "update" do
      it "should update tuple" do
        Tarantool.insert 100, 'привет'
        Tarantool.update 100, ops: [[1, :set, 'yo!']]
        res = Tarantool.select 100
        int, string = res.tuple
        string.to_s.must_equal 'yo!'
      end
    end

    describe "delete" do
      it "should delete record" do
        inserted = Tarantool.insert 100, 'привет', return_tuple: true
        Tarantool.delete inserted.tuple[0], return_tuple: true
        res = Tarantool.select 100
        res.tuple.must_be_nil
      end
    end

    describe "ping" do
      it "should ping without exceptions" do
        res = Tarantool.ping
        res.must_be_kind_of Numeric
      end
    end
  end
end