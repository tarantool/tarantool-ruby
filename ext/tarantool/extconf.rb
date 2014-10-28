begin
  if RUBY_ENGINE != 'ruby' || "\x01\x02\x03\x04".unpack('L') != [0x04030201]
    raise ':('
  end
  require 'mkmf'
  have_func('rb_str_drop_bytes')
  create_makefile("response_c")
rescue
  File.open(File.dirname(__FILE__) + "/Makefile", 'w') do |f|
    f.write("install:\n\t#nothing to build")
  end
end
