# -*- coding: utf-8 -*-
require File.expand_path('../shared_record', __FILE__)
require 'tarantool/light_record'

describe 'Tarantool::LightRecord' do
  let(:base_class){ Tarantool::LightRecord }
  it_behaves_like :record
end
