local log = require 'log'
box.cfg{
    listen = 33013,
    memtx_dir='.',
    wal_mode='none',
}
box.once('initbox', function()
local s1 = box.schema.space.create('test', {id = 513, if_not_exists = true})
local ip = s1:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}, if_not_exists = true})
local iname = s1:create_index('name', {type = 'tree', parts = {2, 'STR'}, if_not_exists = true})
local irtree = s1:create_index('point', {type = 'rtree', unique=false, parts = {3, 'ARRAY'}, if_not_exists = true})
local ipname = s1:create_index('id_name', {type = 'tree', parts = {1, 'unsigned', 2, 'STR'}})
local s2 = box.schema.space.create('test1', {id = 514, if_not_exists = true})
local ip = s2:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}, if_not_exists = true})

box.schema.user.create('tester', {password='testpass'})
box.schema.user.grant('tester', 'read,write,execute', 'universe')
box.schema.func.create('reseed')
box.schema.func.create('func1')
end)

function reseed()
    local s1 = box.space.test
    s1:truncate()
    s1:insert{1, "hello", {1, 2}, 100}
    s1:insert{2, "world", {3, 4}, 200}
    local s2 = box.space.test1
    s2:truncate()
    s2:insert{1, "hello", {1, 2}, 100}
    s2:insert{2, "world", {3, 4}, 200}
end

function func1(i)
    return i+1
end

box.schema.user.grant('guest', 'read,write,execute', 'universe')
local console = require 'console'
console.listen '0.0.0.0:33015'

