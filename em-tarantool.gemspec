# -*- encoding: utf-8 -*-
require File.expand_path('../lib/em-tarantool/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Sokolov Yura 'funny-falcon'"]
  gem.email         = ["funny.falcon@gmail.com"]
  gem.description   = %q{EventMachine callback specific interface to Tarantool}
  gem.summary       = %q{Interface to Tarantool, but using callback interface and without active_model }
  gem.homepage      = ""

  gem.files         = (Dir['lib/**/*'] + Dir['test/*'] + ['test/tarant/init.lua'] +
                      %w{Gemfile Gemfile.lock LICENSE Rakefile README.md}).find_all{|f| File.file?(f)}
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "em-tarantool"
  gem.require_paths = ["lib"]
  gem.version       = EM::Tarantool::VERSION
  gem.add_dependency "iproto", [">= 0.3.2"]
end
