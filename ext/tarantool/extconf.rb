if RUBY_ENGINE == 'ruby' and "\x01\x02\x03\x04".unpack('L') == [0x04030201]
  require 'mkmf'
  have_func('rb_str_drop_bytes')
  create_makefile("response_c")
else
  File.open(File.dirname(__FILE__) + "/Makefile", 'w') do |f|
    f.write("install:\n\t#nothing to build")
  end
end
