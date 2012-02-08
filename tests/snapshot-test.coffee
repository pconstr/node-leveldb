assert  = require 'assert'
leveldb = require '../lib'
path    = require 'path'


describe 'snapshot', ->
  db = null
  filename = "#{__dirname}/../tmp/snapshot-test-file"
  snapshot = null
  key = "Hello"
  val = "World"

  it 'should open database', (done) ->
    leveldb.open filename, create_if_missing: true, (err, handle) ->
      assert.ifError err
      db = handle
      done()

  it 'should create a snapshot', ->
    snapshot = db.snapshot()

  it 'should put key/value pair', ->
    db.putSync key, val, sync: true

  it 'should get key/value pair', (done) ->
    db.get key, (err, result) ->
      assert.ifError err
      assert.equal result, val.toString()
      done()

  it 'should not get snapshot key/value pair', (done) ->
    snapshot.get key, (err, result) ->
      assert.ifError err
      assert.equal result, undefined
      done()

  it 'should close database', (done) ->
    db = null
    snapshot = null
    process.nextTick done

  it 'should destroy database', (done) ->
    leveldb.destroy filename, done
