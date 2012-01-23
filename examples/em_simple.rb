require 'bundler'
ENV['BUNDLE_GEMFILE'] = File.expand_path('../../Gemfile', __FILE__)
Bundler.setup

require 'tarantool'

require 'em-synchrony'
DB = Tarantool.new host: 'localhost', port: 33013
space = DB.space 0
EM.synchrony do
  Fiber.new do
    space.insert 'prepor', 'Andrew', 'ceo@prepor.ru'
    res = space.select 'prepor'
    puts "Name: #{res.tuple[1].to_s}; Email: #{res.tuple[2].to_s}"
    space.delete 'prepor'
    EM.stop
  end.resume
end