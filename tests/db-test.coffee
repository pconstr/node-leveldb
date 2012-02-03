assert  = require 'assert'
leveldb = require '../lib'
path    = require 'path'


describe 'db', ->
  db = null
  filename = "#{__dirname}/../tmp/db-test-file"

  itShouldBehaveLikeAKeyValueStore = (key, val) ->

    it 'should open database', (done) ->
      leveldb.open filename, create_if_missing: true, paranoid_checks: true, (err, handle) ->
        assert.ifError err
        db = handle
        done()

    it 'should put key/value pair', (done) ->
      db.put key, val, done

    it 'should get key/value pair', (done) ->
      db.get key, (err, result) ->
        assert.ifError err
        assert.equal val.toString(), result
        done()

    it 'should delete key', (done) ->
      db.del key, done

    it 'should not get key/value pair', (done) ->
      db.get key, (err, result) ->
        assert.ifError err
        assert.equal undefined, result
        done()

    it 'should close database', (done) ->
      db.close done

    it 'should repair database', (done) ->
      leveldb.repair filename, (err) ->
        assert.ifError err
        assert path.existsSync filename
        done()

    it 'should destroy database', (done) ->
      leveldb.destroy filename, done

  describe 'admin', ->

    it 'should open database', (done) ->
      leveldb.open filename, create_if_missing: true, (err, handle) ->
        assert.ifError err
        db = handle
        done()

    it 'should get property', ->
      assert db.property 'leveldb.stats'
      assert.equal undefined, db.property ''

    it 'should get no approximate size', ->
      assert.equal 0, db.approximateSizes []

    it 'should get one approximate size', ->
      db.approximateSizes '0', '1'

    it 'should put values', (done) ->
      db.put '' + i, 'Hello World!' for i in [0..999]
      db.put '100', 'Goodbye World!', done

    it 'should close database', (done) ->
      db.close done
      db = null

    it 'should open database', (done) ->
      leveldb.open filename, (err, handle) ->
        assert.ifError err
        db = handle
        done()

    it 'should get value', (done) ->
      db.get '100', (err, value) ->
        assert.equal 'Goodbye World!', value
        done()

    it 'should get approximate sizes', ->
      db.approximateSizes '0', '1000'

    it 'should get approximate sizes', ->
      db.approximateSizes [[ '0', '50' ], [ '50', '1000' ]]

    it 'should close database', (done) ->
      db.close ->
        db = null
        done()

  describe 'with ascii values', ->
    itShouldBehaveLikeAKeyValueStore "Hello", "World"

  describe 'with buffer values', ->
    key = new Buffer [1,9,9,9]
    val = new Buffer [1,2,3,4]
    itShouldBehaveLikeAKeyValueStore key, val
