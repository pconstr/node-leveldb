assert  = require 'assert'
leveldb = require '../lib'
path    = require 'path'


describe 'snapshot', ->
  filename = "#{__dirname}/../tmp/snapshot-test-file"
  db = null
  snapshot = null

  key = "Hello"
  val = "World"

  beforeEach ->
    db = leveldb.open filename, create_if_missing: true
    db.putSync key, val
    assert.equal val, db.get key

  afterEach (done) ->
    db = null
    snapshot = null
    leveldb.destroy filename, done

  describe 'async', ->

    beforeEach (done) ->
      db.snapshot (err, snap) ->
        snapshot = snap
        done err

    it 'should get value', (done) ->
      db.putSync key, 'Goodbye'
      assert.equal 'Goodbye', db.get key
      snapshot.get key, (err, value) ->
        assert.ifError err
        assert.equal val, value
        done()

    it 'should not deleted value', (done) ->
      db.delSync key
      assert.ifError db.get key
      snapshot.get key, (err, value) ->
        assert.ifError err
        assert.equal val, value
        done()

  describe 'sync', ->

    beforeEach ->
      snapshot = db.snapshot()

    it 'should get value', ->
      db.putSync key, 'Goodbye'
      assert.equal 'Goodbye', db.get key
      assert.equal val, snapshot.get key

    it 'should not deleted value', ->
      db.delSync key
      assert.ifError db.get key
      assert.equal val, snapshot.get key
