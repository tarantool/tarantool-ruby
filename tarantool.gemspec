# -*- encoding: utf-8 -*-
require File.expand_path('../lib/tarantool/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name          = "tarantool"
  gem.version       = Tarantool::VERSION
  gem.summary       = "Tarantool KV-storage client."
  gem.description   = "Tarantool KV-storage client."
  gem.homepage      = "http://github.com/mailru/tarantool-ruby"
  gem.authors       = ["Andrew Rudenko", "Sokolov Yura 'funny-falcon'"]
  gem.email         = ["ceo@prepor.ru", "funny.falcon@gmail.com"]

  gem.rdoc_options = ["--charset=UTF-8"]
  gem.extra_rdoc_files = %w[README.md LICENSE]

  gem.files         = (Dir['lib/**/*'] + Dir['test/*'] + ['test/tarant/init.lua'] +
                      %w{Gemfile Gemfile.lock LICENSE Rakefile README.md}).find_all{|f| File.file?(f)}
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_dependency "iproto", [">= 0.3.6"]
end
