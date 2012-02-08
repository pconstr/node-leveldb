assert  = require 'assert'
leveldb = require '../lib'
path    = require 'path'




describe 'batch', ->
  filename = "#{__dirname}/../tmp/batch-test-file"

  db = leveldb.openSync filename, create_if_missing: true

  value = 'Hello World'
  newValue = 'Goodbye World'

  it 'should insert data (async)', (done) ->
    batch = new leveldb.Batch
    batch.put "#{i}", value for i in [100..199]
    db.write batch, sync: true, done

  it 'should insert data (sync)', ->
    batch = new leveldb.Batch
    batch.put "#{i}", value for i in [200..299]
    db.writeSync batch

  it 'should get values', ->
    for i in [100..299]
      val = db.getSync "#{i}"
      assert.equal value, val

  it 'should delete data (async)', (done) ->
    batch = new leveldb.Batch
    batch.del "#{i}" for i in [100..149]
    db.write batch, sync: true, done

  it 'should delete data (sync)', ->
    batch = new leveldb.Batch
    batch.del "#{i}" for i in [150..199]
    db.writeSync batch

  it 'should not get values', ->
    for i in [100..199]
      val = db.getSync "#{i}"
      assert.ifError val

  it 'should replace data (async)', (done) ->
    batch = db.batch()
    for i in [200..249]
      batch.del "#{i}"
      batch.put "k#{i}", newValue
    batch.write sync: true, done

  it 'should replace data (sync)', ->
    batch = db.batch()
    for i in [250..299]
      batch.del "#{i}"
      batch.put "k#{i}", newValue
    batch.writeSync batch

  it 'should not get values', ->
    for i in [200..299]
      val = db.getSync "#{i}"
      assert.ifError val

  it 'should get values', ->
    for i in [200..299]
      val = db.getSync "k#{i}"
      assert.equal newValue, val
