assert  = require 'assert'
leveldb = require '../lib'
path    = require 'path'




describe 'iterator', ->
  db = null
  filename = "#{__dirname}/../tmp/iterator-test-file"
  iterator = null
  value = 'Hello World'

  it 'should open database', ->
    db = leveldb.openSync filename, create_if_missing: true

  it 'should insert batch data', ->
    batch = new leveldb.Batch
    batch.put "#{i}", value for i in [100..200]
    db.writeSync batch

  it 'should get an iterator', ->
    assert iterator = db.iterator()

  it 'should get values', ->
    for i in [100..200]
      [key, val] = iterator.nextSync()
      assert.equal "#{i}", key
      assert.equal value, val

  it 'should close database', ->
    db.closeSync()

  it 'should destroy database', ->
    leveldb.destroySync filename
