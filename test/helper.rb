$: << File.expand_path('../../lib', __FILE__)
require 'minitest/spec'
require 'fileutils'
require 'tarantool16'
require 'socket'


class Spawn
  DIR = File.expand_path('..', __FILE__)
  include FileUtils
  Dir[File.join(DIR, "tarantool*.log")].each{|f| File.unlink(f)}

  attr :port
  def initialize(opts={})
    @name = opts[:name] or raise "no name"
    @port = opts[:port] || 16788
    @admin_port = opts[:admin_port] || 16789
    @binary = opts[:binary] || 'tarantool'
    @instance = opts[:instance] || 'main'
    @config = opts[:config] || 'config.lua'
  end

  def fjoin(*args)
    File.join(*args.map(&:to_s))
  end

  def dir
    fjoin(DIR, "run", ["tarantool", @instance].compact.join('_'))
  end

  def clear
    return unless @dir
    stop
    rm_rf(dir)
  end

  def stop
    return unless @pid
    Process.kill('INT', @pid)
    Process.wait2(@pid)
    @pid = nil
  end

  def prepare
    return if @dir
    clear
    @dir = dir
    mkdir_p(@dir)
    conf = File.read(fjoin(DIR, @config))
    conf.sub!(/listen = \d+/, "listen = #{@port}")
    conf.sub!(/listen '0\.0\.0\.0:\d+'/, "listen '0.0.0.0:#{@admin_port}'")
    if block_given?
      yield conf
    end
    File.open(fjoin(@dir, 'config.lua'), 'w'){|f| f.write(conf)}
  end

  def run
    return if @pid
    prepare
    log = File.open(fjoin(DIR, "tarantool_#{@name}.log"), 'ab')
    Dir.chdir(@dir) do
      @pid = spawn(@binary, 'config.lua', {out: log, err: log})
    end
    sleep(0.2)
  end

  def reseed
    run
    s = TCPSocket.new '127.0.0.1', @admin_port
    s.send("reseed()\n", 0)
    s.close
  end

  CONF = {
    main: {conf:{}}
  }

  AT_EXIT_CALLED = [false]
  def self.inst(what=:main)
    CONF[what][:inst] ||= new(CONF[what][:conf].merge(name: what.to_s))
    CONF[what][:inst]
  end

  def self.run(what=:main)
    inst(what).run
  end

  def self.reseed(what=:main)
    inst(what).reseed
  end

  def self.stop(what=:main)
    if inst = CONF[what][:inst]
      inst.stop
    end
  end

  def self.clear(what=:main)
    if inst = CONF[what][:inst]
      inst.clear
    end
  end

  at_exit do
    CONF.each_key{|name| clear(name)}
    FileUtils.rm_rf(File.join(DIR, "run"))
  end
end

require 'minitest/autorun'
