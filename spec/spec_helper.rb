require 'bundler'
ENV['BUNDLE_GEMFILE'] = File.expand_path('../../Gemfile', __FILE__)
Bundler.setup

require 'minitest/spec'
require 'minitest/autorun'

require 'helpers/let'
require 'helpers/truncate'
require 'rr'

require 'tarantool'

TARANTOOL_CONFIG = { host: '127.0.0.1', port: 33013, type: :block }

DB = Tarantool.new TARANTOOL_CONFIG

class MiniTest::Unit::TestCase
  extend Helpers::Let
  include RR::Adapters::MiniTest
end