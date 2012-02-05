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

  it 'should open database', (done) ->
    leveldb.open filename, (err, handle) ->
      assert.ifError err
      db = handle
      done()

  it 'should get an iterator', (done) ->
    db.iterator (err, it) ->
      assert.ifError err
      assert iterator = it
      done()

  it 'should get values', (done) ->
    testNext = (i) ->
      iterator.next (err, key, val) ->
        assert.ifError err
        assert.equal "#{i}", key
        assert.equal value, val
        if ++i <= 200
          testNext i
        else
          done()

    testNext 100

  it 'should not be valid', ->
    assert.ifError iterator.valid()

  it 'should seek to key 201', (done) ->
    iterator.seek '201', done

  it 'should not be valid', ->
    assert.ifError iterator.valid()

  it 'should seek to key 200', (done) ->
    iterator.seek '200', done

  it 'should get values in reverse', (done) ->
    testNext = (i) ->
      iterator.next (err, key, val) ->
        assert.ifError err
        assert.equal "#{i}", key
        assert.equal value, val
        if --i >= 100
          testNext i
        else
          done()

    testNext 200

  it 'should not be valid', ->
    assert.ifError iterator.valid()

  it 'should close database', (done) ->
    db.close done
    db = null

  it 'should not get values', ->
    assert.throws -> iterator.first()

describe 'iterator (sync)', ->
  db = null
  filename = "#{__dirname}/../tmp/iterator-async-test-file"
  iterator = null
  value = 'Hello World'

  it 'should open database', ->
    db = leveldb.openSync filename, create_if_missing: true

  it 'should insert batch data', ->
    batch = new leveldb.Batch
    batch.put "#{i}", value for i in [100..200]
    db.writeSync batch

  it 'should get an iterator', ->
    assert iterator = db.iteratorSync()

  it 'should get values', ->
    for i in [100..200]
      [key, val] = iterator.nextSync()
      assert.equal "#{i}", key
      assert.equal value, val

  it 'should not be valid', ->
    assert.ifError iterator.valid()

  it 'should seek to key 201', ->
    iterator.seekSync '201'

  it 'should not be valid', ->
    assert.ifError iterator.valid()

  it 'should seek to key 200', ->
    iterator.seekSync '200'

  it 'should get values in reverse', ->
    for i in [200..100]
      [key, val] = iterator.prevSync()
      assert.equal "#{i}", key
      assert.equal value, val

  it 'should not be valid', ->
    assert.ifError iterator.valid()

  it 'should close database', ->
    db.closeSync()
    db = null

  it 'should not get values', ->
    assert.throws -> iterator.firstSync()
