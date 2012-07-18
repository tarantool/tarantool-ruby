# About

Its ruby client for [Tarantool Key-Value Storage](http://github.com/mailru/tarantool).

# Install

```bash
gem install tarantool
```

# Usage

```ruby
require 'tarantool'
```

To be able to send requests to the server, you must initialize Tarantool
and Tarantool space. Space could be initialized with definition of fields
types (and names) or without (which is not recommended).

Available field types:
- `:int`, `:integer` - nonnegative 32 bit integer
- `:int64`, `:integer64` - nonnegative 64 bit integer
- `:varint` - 32bit or 64bit integer, depending on value
- `:str`, `:string` - UTF-8 string (attention: empty string is stored as "\x00", which converted back to "" on load)
- `:bytes`  - ASCII8-bit
- `:auto` - do not use it (used for space without definition)
- any object with #encode and #decode methods

Declaration of indexes is optional for array spaces and required for hash spaces.
When there is no indexes defined for space array, their behaviour is not fixes, so that
is up to you to specify right amount of values for that indexes.

```ruby
DB = Tarantool.new host: 'locahost', port: 33013
space_array_without_definition = DB.space 0

space_array = DB.space 1, [:int, :str, :int], keys: [0, [1,2]]

 # last integer specifies tuples tail pattern
 # note, that two indexes are defined here
space_array_with_tail = DB.space 2, [:int, :str, :str, :int, 2], keys: [0, 1]

 # space, which returns hashes
space_hash = DB.space 1, {id: int, name: :str, score: :int}, keys: [:id, [:name, :score]]
space_hash_with_tail = DB.space 2, {id: int, name: :str, _tail: [:str, :int]}, keys: [:id, :name]
```

The driver internals can work in three modes:
- blocking via TCPSocket
- callback style via EventMachine
- EM::Synchrony like via EventMachine and fibers, so that control flow is visually
  blocked, but eventloop is not (see EM::Synchrony)

```ruby
  DB_SYNC = Tarantool.new host: 'localhost', port: 33013, type: :block
  DB_CALLBACK = Tarantool.new host: 'localhost', port: 33013, type: :em_callback || :em_cb
  DB_FIBER = Tarantool.new host: 'localhost', port: 33013, type: :em_fiber || :em
```

Blocking and Fibered interfaces look similar:

```ruby
  space = (DB_SYNC || DB_FIBER).space 0, [:str, :str, :str], keys: 0
  # EM.synchrony do
    space.insert ['prepor', 'Andrew', 'ceo@prepor.ru']
    res = space.by_pk 'prepor' # || ['prepor']
    res = space.first_by_key 0, 'prepor' # || ['prepor']
    res = space.select 0, ['prepor']
    puts "Name: #{res[1]}; Email: #{res[2]}"
    space.delete 'prepor'
  # EM.stop
  # end
```

Callback interface is a bit different:

```ruby
space = DB_CALLBACK.space 0, [:str, :str, :str], keys: 0
EM.schedule do
  space.insert ['prepor', 'Andrew', 'ceo@prepor.ru'] do |res|
    if Exception === res
      catch_error
    else
      space.by_pk 'prepor' do |res|
        if Exception === res
          catch_error
        else
          puts "Name: #{res[1]}; Email: #{res[2]}"
          space.delete 'prepor' do |res|
            catch_error  if Exception === res
            EM.stop
          end
        end
      end
    end
  end
end
```

**Notice** Blocking `Tarantool` connections are not threadsafe. So, you should create `Tarantool` instance per thread.

## LightRecord

`LightRecord` is a light model with callbacks ala Sequel. It is not aware about ActiveModel goodness.
For ActiveModel avare record look for `tarantool-record` gem

```ruby
require 'tarantool/light_record'
require 'tarantool/serializers/bson'
class User < Tarantool::Record
  field :login, :string
  field :name, :string
  field :email, :string
  field :apples_count, :integer, default: 0
  field :info, :bson
  index :name, :email

  def after_init
    super
    # some work
  end

  def before_create
    # validation could occure here
    super # call super if all is allright, return false otherwise
  end
end

# Now attribute positions are not important.
User.create login: 'prepor', email: 'ceo@prepor.ru', name: 'Andrew'
User.create login: 'ruden', name: 'Andrew', email: 'rudenkoco@gmail.com'

# find by primary key login
User.by_pk 'prepor'
User.first 'prepor'
User.first login: 'prepor'
User.find 'prepor'
# first 2 users with name Andrew
User.all({name: 'Andrew'}, limit: 2)
User.select({name: 'Andrew'}, limit: 2)
User.where(name: 'Andrew').limit(2).all
# second user with name Andrew
User.all({name: 'Andrew'}, offset: 1, limit: 1)[0]
User.select({name: 'Andrew'}, offset: 1, limit: 1)[0]
User.where(name: 'Andrew').offset(1).limit(1).all[0]
User.where(name: 'Andrew').offset(1).first
# user with name Andrew and email ceo@prepor.ru
User.where(name: 'Andrew', email: 'ceo@prepor.ru').first
# raise exception, becouse we can't select query started from not first part of index
begin
    User.where(email: 'ceo@prepor.ru')
rescue Tarantool::ArgumentError => e
end
# increment field apples_count by one. Its atomic operation via native Tarantool interface
User.find('prepor').increment :apples_count

# update all attributes (see tarantool-record gem for record, which updates only dirty attributes)
user = User.find('prepor')
user.name = "Petr"
user.save
user.update_attributes email: "petr@inter.com"  # calls callbacks as well as `save`
user.update email: "petr@inter.com" # do not calls callbacks, and reloads all fields

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

* .to_config
* `#first`, `#all` without keys, batches requests via box.select_range
* `#where` chains
* admin-socket protocol
* safe to add fields to exist model
* Hash, Array and lambdas as default values
* timers to response
