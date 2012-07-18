#!/usr/bin/env rake
require "bundler/gem_helper"
Bundler::GemHelper.install_tasks name: 'tarantool'
namespace :record do
  Bundler::GemHelper.install_tasks name: 'tarantool-record'
end

require 'rake/testtask'
Rake::TestTask.new do |i|
  i.options = '-v'
  i.verbose = true
end
