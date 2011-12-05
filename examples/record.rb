require 'bundler'
ENV['BUNDLE_GEMFILE'] = File.expand_path('../../Gemfile', __FILE__)
Bundler.setup

require 'tarantool/record'
require 'tarantool/serializers/bson'
class User < Tarantool::Record
  field :login, :string
  field :name, :string
  field :email, :string      
  field :apples_count, :integer, default: 0
  field :info, :bson
  index :name, :email

  validates_length_of(:login, minimum: 3)

  after_create do
    # after work!
  end
end

EM.synchrony do
  Tarantool.configure host: 'localhost', port: 33013, space_no: 0
  # Now attribute positions are not important.
  User.create login: 'prepor', email: 'ceo@prepor.ru', name: 'Andrew'
  User.create login: 'ruden', name: 'Andrew', email: 'rudenkoco@gmail.com'

  # find by primary key login
  User.find 'prepor' 
  # first 2 users with name Andrew
  User.where(name: 'Andrew').limit(2).all 
  # second user with name Andrew
  User.where(name: 'Andrew').offset(1).limit(1).all 
  # user with name Andrew and email ceo@prepor.ru
  User.where(name: 'Andrew', email: 'ceo@prepor.ru').first
  # raise exception, becouse we can't select from not first part of index
  begin
    User.where(email: 'ceo@prepor.ru') 
  rescue Tarantool::ArgumentError => e
  end
  # increment field apples_count by one. Its atomic operation vie native Tarantool interface
  User.find('prepor').increment :apples_count

  # update only dirty attributes
  user = User.find('prepor')
  user.name = "Petr"
  user.save

  # field serialization to bson
  user.info = { 'bio' => "hi!", 'age' => 23, 'hobbies' => ['mufa', 'tuka'] }
  user.save
  User.find('prepor').info['bio'] # => 'hi!'
  user.destroy
  puts "Ok!"
  EM.stop
end