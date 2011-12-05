require 'bundler'
ENV['BUNDLE_GEMFILE'] = File.expand_path('../../Gemfile', __FILE__)
Bundler.setup

require 'tarantool'

EM.run do
  Tarantool.configure host: 'localhost', port: 33013, space_no: 0
  req = Tarantool.insert 'prepor', 'Andrew', 'ceo@prepor.ru'
  req.callback do
    req = Tarantool.select 'prepor'
    req.callback do |res|
      puts "Name: #{res.tuple[1].to_s}; Email: #{res.tuple[2].to_s}"
      EM.stop
    end
  end
  req.errback do |err|
    puts "Error while insert: #{err}"
    EM.stop
  end
end