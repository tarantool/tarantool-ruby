# -*- encoding: utf-8 -*-
require File.expand_path('../lib/tarantool/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Sokolov Yura 'funny-falcon'"]
  gem.email         = ["funny.falcon@gmail.com"]
  gem.description   = %q{Ruby interface to tarantool}
  gem.summary       = %q{Interface to Tarantool tarantool.org}
  gem.homepage      = ""

  gem.files         = (Dir['lib/**/*'] + Dir['test/*'] + ['test/tarant/init.lua'] +
                      %w{Gemfile Gemfile.lock LICENSE Rakefile README.md}).find_all{|f| File.file?(f)}
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "tarantool"
  gem.require_paths = ["lib"]
  gem.version       = Tarantool::VERSION
  gem.add_dependency "iproto", [">= 0.3.3"]
end
