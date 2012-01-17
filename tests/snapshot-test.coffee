assert  = require 'assert'
leveldb = require '../lib'
path    = require 'path'




describe 'snapshot', ->
  db = new leveldb.DB
  filename = "#{__dirname}/../tmp/snapshot-test-file"
  snapshot = null
  key = "Hello"
  val = "World"

  it 'should open database', (done) ->
    db.open filename, create_if_missing: true, done

  it 'should create a snapshot', ->
    snapshot = db.getSnapshot()

  it 'should have a snapshot', ->
    assert snapshot.valid()

  it 'should put key/value pair', (done) ->
    db.put key, val, done

  it 'should get key/value pair', (done) ->
    db.get key, (err, result) ->
      assert.ifError err
      assert.equal result, val.toString()
      done()

  it 'should not get snapshot key/value pair', (done) ->
    db.get key, snapshot: snapshot, (err, result) ->
      assert.ifError err
      assert.equal result, undefined
      done()

  it 'should release snapshot', ->
    snapshot.close()

  it 'should close database', (done) ->
    db.close done

  it 'should destroy database', ->
    result = leveldb.DB.destroyDB filename
    assert.equal result, 'OK'
