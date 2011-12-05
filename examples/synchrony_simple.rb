require 'bundler'
ENV['BUNDLE_GEMFILE'] = File.expand_path('../../Gemfile', __FILE__)
Bundler.setup

require 'tarantool/synchrony'

EM.synchrony do
  Tarantool.configure host: 'localhost', port: 33013, space_no: 0
  Tarantool.insert 'prepor', 'Andrew', 'ceo@prepor.ru'
  res = Tarantool.select 'prepor'
  puts "Name: #{res.tuple[1].to_s}; Email: #{res.tuple[2].to_s}"
  EM.stop
end