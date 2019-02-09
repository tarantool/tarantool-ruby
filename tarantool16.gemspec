# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'tarantool16/version'

Gem::Specification.new do |spec|
  spec.name          = "tarantool16"
  spec.version       = Tarantool16::VERSION
  spec.authors       = ["Sokolov Yura aka funny_falcon"]
  spec.email         = ["funny.falcon@gmail.com"]
  spec.summary       = %q{adapter for Tarantool 1.6}
  spec.description   = %q{adapter for Tarantool 1.6}
  spec.homepage      = "https://github.com/funny-falcon/tarantool16-ruby"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "minitest", ">= 0.0.0"

  spec.add_dependency "msgpack", ">= 0.5.11"
end
