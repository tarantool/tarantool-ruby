# About

Its asynchronyous EventMachine ruby client for [Tarantool Key-Value Storage](http://github.com/mailru/tarantool).

# Install

```bash
gem install tarantool
```

```ruby
require 'tarantool'
# or
require 'tarantool/synchrony'
# or
require 'tarantool/record'
```

# Usage

To be able to send requests to the server, you must
configure and establish a client connection:

```ruby
Tarantool.configure host: 'locahost', port: 33013, space_no: 0
```

The driver internals can work in two modes: EM deferrables and EM-Synchrony.

EM deferrables:

```ruby
req = Tarantool.insert 'prepor', 'Andrew', 'ceo@prepor.ru'
req.callback do
    req = Tarantool.select 'prepor'
    req.callback do |res|
        puts "Name: #{res.tuple[1].to_s}; Email: #{res.tuple[2].to_s}"
    end
end
req.errback do |err|
    puts "Error while insert: #{err}"
end
```

'Synchrony' mode:

```ruby
require 'tarantool/synchrony'

Tarantool.insert 'prepor', 'Andrew', 'ceo@prepor.ru'
res = Tarantool.select 'prepor'
puts "Name: #{res.tuple[1].to_s}; Email: #{res.tuple[2].to_s}"
```

The driver itself provides ActiveModel API: Callbacks, Validations, Serialization, Dirty. 
Type casting is automatic, based on the index type chosen to process the query.
For example:

```ruby
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
# raise exception, becouse we can't select query started from not first part of index
begin
    User.where(email: 'ceo@prepor.ru') 
rescue Tarantool::ArgumentError => e
end
# increment field apples_count by one. Its atomic operation via native Tarantool interface
User.find('prepor').increment :apples_count

# update only dirty attributes
user = User.find('prepor')
user.name = "Petr"
user.save

# field serialization to bson
user.info = { 'bio' => "hi!", 'age' => 23, 'hobbies' => ['mufa', 'tuka'] }
user.save
User.find('prepor').info['bio'] # => 'hi!'
# delete record
user.destroy
```

When definining a record, field order is important: this is the order of fields
in the tuple stored by Tarantool. By default, the primary key is field 0. 

`index` method just mapping to your Tarantool schema, client doesn't modify schema for you.

# TODO

* `#first`, `#all` without keys, batches requests via box.select_range
* `#where` chains
* admin-socket protocol
* safe to add fields to exist model
* Hash, Array and lambdas as default values
* timers to response, reconnect strategies