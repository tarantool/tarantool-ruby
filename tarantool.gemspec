Gem::Specification.new do |s|
  s.specification_version = 2 if s.respond_to? :specification_version=
  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.rubygems_version = '1.3.5'

  s.name              = 'tarantool'
  s.version           = '0.1.1'
  s.date              = Time.now.strftime('%Y-%m-%d')
  s.rubyforge_project = 'tarantool'

  s.summary     = "Tarantool KV-storage client."
  s.description = "Tarantool KV-storage client."

  s.authors  = ["Andrew Rudenko"]
  s.email    = 'ceo@prepor.ru'
  s.homepage = 'http://github.com/mailru/tarantool-ruby'

  s.require_paths = %w[lib]

  s.rdoc_options = ["--charset=UTF-8"]

  s.add_dependency('activemodel', [">= 3.1", "< 4.0"])
  # I suppose that there dependencies are optional, isn't it?
  s.add_development_dependency('eventmachine', [">= 1.0.0.beta.4", "< 2.0.0"])
  s.add_development_dependency('em-synchrony', [">= 1.0.0", "< 2.0"])

  # = MANIFEST =
  s.executables = `git ls-files -- bin/*`.split("\n").map { |f| File.basename(f) }
  s.extra_rdoc_files = %w[README.md LICENSE]
  s.files = `git ls-files`.split("\n")
  s.test_files = `git ls-files -- {test,spec,features}/*`.split("\n")
  # = MANIFEST =
end
