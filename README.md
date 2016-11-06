# Tarantool16

This is adapter for [tarantool](http://tarantool.org) version 1.6 and 1.7.

(adapter for version <=1.5 is called [tarantool](https://github.org/tarantoool/tarantool-ruby))

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'tarantool16'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install tarantool16

## Usage

Currently only simple single threaded one-request-at-time connection implemented.

```ruby
require 'tarantool16'

db = Tarantool16.new host:'localhost:33013'
#db = Tarantool16.new host:'localhost:33013', user:'tester', password:'testpass'
#db = Tarantool16.new host:['tcp','localhost:33013']
#db = Tarantool16.new host:['unix','path/to.sock']
#db = Tarantool16.new unix:'path/to.sock'

# select from '_space' space info about 'test' table
# returns array of tuples as an array
tar.get(272, ['test'], index: 2)

# same, but return tuples as a hashes
tar.get(272, ['test'], index: 2, hash: true)

# same with names
# Names and index descriptions are fetched from tarantool.
# Index is autodetected by key names
tar.get(:_space, {name: 'test'})

# get all spaces
tar.select(:_space, nil, iterator: :all)
tar.select(:_space, nil, iterator: :all, hash: true)

tar.select(:_space, [512], index: 0, iterator: :>=, hash: true)

# override tuple field definition
tar.define_fields(:test, [:id, :name, :value])

tar.insert(:test, [1, 'buddy', [1,2,3]])
tar.replace(:test, [1, 'buddy!', [2,3,4]])
tar.update(:test, [1], [[':', 1, 6, 6, '?']])
#tar.update(:test, [1], [[':', 1, 6, 6, '?']], index: 0)
tar.delete(:test, [1])
#tar.delete(:test, [1], index: 0)

tar.insert(:test, {id: 1, name: 'buddy', value: [1,2,3]})
tar.replace(:test, {id: 1, name: 'buddy!', value: [2,3,4]})
tar.update(:test, {id: 1}, {name: [':', 6,6,'?']})
tar.delete(:test, {id: 1})

# note: currenlty there is no documented way to store field definition in an tarantool
# but actually you can do it with this
tar.update(:_space, {name: 'test'}, {format: [:=, [{name: :id, type: :num}, {name: :name, type: :str}, {name: :value, type: '*'}]]})

```

## Changelog

0.1.0 - breaking change: call now issues request in Tarantool17 format.
        use call16 to issue `call` request to Tarantool16.
	see tests for difference in returned results.
0.0.11 - change a way of unix socket option.
        since previos scheme were introduced 12 hours ago,
        i think it is still safe to change it.
0.0.10 - a bit of fixes
0.0.9 - Fix detection of update operation shape
        Add unix socket connection.
0.0.8 - Fix schema read from new tarantool versions
        Implement UPSERT
0.0.7 - Implement EVAL, fix REPLACE
0.0.6 - DumbConnection supports timeouts

## Contributing

1. Fork it ( https://github.com/funny-falcon/tarantool16/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

Or simply fill an issue
