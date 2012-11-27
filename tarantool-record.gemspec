# -*- encoding: utf-8 -*-
require File.expand_path('../lib/tarantool/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name          = "tarantool-record"
  gem.version       = Tarantool::RECORD_VERSION
  gem.summary       = "Tarantool KV-storage ActiveModel-aware Record."
  gem.description   = "Tarantool KV-storage ActiveModel-aware Record."
  gem.homepage      = "http://github.com/mailru/tarantool-ruby"
  gem.authors       = ["Andrew Rudenko", "Sokolov Yura 'funny-falcon'"]
  gem.email         = ["ceo@prepor.ru", "funny.falcon@gmail.com"]

  gem.rdoc_options = ["--charset=UTF-8"]
  gem.extra_rdoc_files = %w[README_RECORD.md LICENSE]

  files             = %w{lib/tarantool/record.rb test/test_record.rb LICENSE README_RECORD.md}
  gem.files         = files

  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_dependency "tarantool", ["~> #{Tarantool::VERSION}"]
  gem.add_dependency "activemodel", [">= 3.1", "< 4.0"]
end
