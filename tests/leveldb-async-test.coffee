assert  = require 'assert'
leveldb = require '../lib'





describe 'db', ->
  db = new leveldb.DB
  path = "#{__dirname}/../tmp/db-test-file"
  key = "Hello"
  value = "World"

  it 'should open database', (done) ->
    db.open path, { create_if_missing: true, paranoid_checks: true }, done

  it 'should put key/value pair', (done) ->
    db.put key, value, done

  it 'should get key/value pair', (done) ->
    db.get key, (err, result) ->
      assert.ifError err
      assert.equal result, value
      done()

  it 'should delete key', (done) ->
    db.del key, done

  it 'should not get key/value pair', (done) ->
    db.get key, (err, result) ->
      assert.ifError err
      assert.equal result, undefined
      done()

  it 'should close database', (done) ->
    db.close done
