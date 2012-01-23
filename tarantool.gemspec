Gem::Specification.new do |s|
  s.specification_version = 2 if s.respond_to? :specification_version=
  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.rubygems_version = '1.3.5'

  s.name              = 'tarantool'
  s.version           = '0.2'
  s.date              = '2012-01-23'
  s.rubyforge_project = 'tarantool'

  s.summary     = "Tarantool KV-storage client."
  s.description = "Tarantool KV-storage client."

  s.authors  = ["Andrew Rudenko"]
  s.email    = 'ceo@prepor.ru'
  s.homepage = 'http://github.com/mailru/tarantool-ruby'

  s.require_paths = %w[lib]

  s.rdoc_options = ["--charset=UTF-8"]
  s.extra_rdoc_files = %w[README.md LICENSE]

  s.add_dependency('iproto', [">= 0.1"])
  s.add_dependency('activemodel', [">= 3.1", "< 4.0"])


  # = MANIFEST =
  s.files = %w[
    Gemfile
    Gemfile.lock
    LICENSE
    README.md
    Rakefile
    examples/em_simple.rb
    examples/record.rb
    examples/simple.rb
    lib/tarantool.rb
    lib/tarantool/exceptions.rb
    lib/tarantool/record.rb
    lib/tarantool/request.rb
    lib/tarantool/requests.rb
    lib/tarantool/requests/call.rb
    lib/tarantool/requests/delete.rb
    lib/tarantool/requests/insert.rb
    lib/tarantool/requests/ping.rb
    lib/tarantool/requests/select.rb
    lib/tarantool/requests/update.rb
    lib/tarantool/response.rb
    lib/tarantool/serializers.rb
    lib/tarantool/serializers/bson.rb
    lib/tarantool/serializers/integer.rb
    lib/tarantool/serializers/string.rb
    lib/tarantool/space.rb
    spec/helpers/let.rb
    spec/helpers/truncate.rb
    spec/spec_helper.rb
    spec/tarantool.cfg
    spec/tarantool/em_spec.rb
    spec/tarantool/record_spec.rb
    spec/tarantool/request_spec.rb
    tarantool.gemspec
  ]
  # = MANIFEST =

  ## Test files will be grabbed from the file list. Make sure the path glob
  ## matches what you actually use.
  s.test_files = s.files.select { |path| path =~ /^spec\/.*_spec\.rb/ }
end