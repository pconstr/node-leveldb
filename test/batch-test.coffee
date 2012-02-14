assert  = require 'assert'
leveldb = require '../lib'
path    = require 'path'




describe 'Batch', ->
  filename = "#{__dirname}/../tmp/batch-test-file"
  db = null

  async = (test, callback) -> (err) ->
    assert.ifError err
    test()
    callback()

  hasPut = ->
    assert.equal "Goodbye #{i}", db.get "#{i}" for i in [100..119]

  hasDel = ->
    assert.ifError db.get "#{i}" for i in [180..199]

  hasBoth = ->
    assert.equal "Goodbye #{i}", db.get "#{i}" for i in [100..119]
    assert.ifError db.get "#{i}" for i in [180..199]

  hasNoop = ->
    assert.ifError db.get "#{i}" for i in [100..109]
    assert.equal "Hello #{i}", db.get "#{i}" for i in [110..189]
    assert.ifError db.get "#{i}" for i in [190..199]

  # populate fresh database
  beforeEach ->
    db = leveldb.open filename, create_if_missing: true, error_if_exists: true
    db.putSync "#{i}", "Hello #{i}" for i in [110..189]
    hasNoop()

  # close and destroy database
  afterEach (done) ->
    db = null
    leveldb.destroy filename, done

  describe 'async new', ->
    b = null

    it 'should put()', (done) ->
      batch = new leveldb.Batch
      batch.put "#{i}", "Goodbye #{i}" for i in [100..119]
      db.write batch, async hasPut, done

    it 'should del()', (done) ->
      batch = new leveldb.Batch
      batch.del "#{i}" for i in [180..189]
      db.write batch, async hasDel, done

    it 'should put() del()', (done) ->
      b = batch = new leveldb.Batch
      batch.put "#{i}", "Goodbye #{i}" for i in [100..119]
      batch.del "#{i}" for i in [180..189]
      db.write batch, async hasBoth, done

    it 'should put() del() again', (done) ->
      db.write b, async hasBoth, done

    it 'should not put() del() after clear()', (done) ->
      b.clear()
      db.write b, async hasNoop, done

  describe 'sync new', ->
    b = null

    it 'should put()', ->
      batch = new leveldb.Batch
      batch.put "#{i}", "Goodbye #{i}" for i in [100..119]
      db.writeSync batch
      hasPut()

    it 'should del()', ->
      batch = new leveldb.Batch
      batch.del "#{i}" for i in [180..189]
      db.writeSync batch
      hasDel()

    it 'should put() del()', ->
      b = batch = new leveldb.Batch
      batch.put "#{i}", "Goodbye #{i}" for i in [100..119]
      batch.del "#{i}" for i in [180..189]
      db.writeSync batch
      hasBoth()

    it 'should put() del() again', (done) ->
      db.write b, async hasBoth, done

    it 'should not put() del() after clear()', (done) ->
      b.clear()
      db.write b, async hasNoop, done

  describe 'async db.batch()', ->
    b = null

    it 'should put()', (done) ->
      batch = db.batch()
      batch.put "#{i}", "Goodbye #{i}" for i in [100..119]
      batch.write async hasPut, done

    it 'should del()', (done) ->
      batch = db.batch()
      batch.del "#{i}" for i in [180..189]
      batch.write async hasDel, done

    it 'should put() del()', (done) ->
      b = batch = db.batch()
      batch.put "#{i}", "Goodbye #{i}" for i in [100..119]
      batch.del "#{i}" for i in [180..189]
      batch.write async hasBoth, done

    it 'should not put() del() again', (done) ->
      db.write b, async hasNoop, done

  describe 'sync db.batch()', ->
    b = null

    it 'should put()', ->
      batch = db.batch()
      batch.put "#{i}", "Goodbye #{i}" for i in [100..119]
      batch.writeSync()
      hasPut()

    it 'should del()', ->
      batch = db.batch()
      batch.del "#{i}" for i in [180..189]
      batch.writeSync()
      hasDel()

    it 'should put() del()', ->
      b = batch = db.batch()
      batch.put "#{i}", "Goodbye #{i}" for i in [100..119]
      batch.del "#{i}" for i in [180..189]
      batch.writeSync()
      hasBoth()

    it 'should not put() del() again', (done) ->
      db.write b, async hasNoop, done
