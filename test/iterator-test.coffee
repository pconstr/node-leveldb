assert  = require 'assert'
leveldb = require '../lib'
path    = require 'path'




describe 'iterator', ->
  filename = "#{__dirname}/../tmp/iterator-test-file"
  db = null
  iterator = null

  beforeEach ->
    db = leveldb.open filename, create_if_missing: true, error_if_exists: true
    batch = db.batch()
    batch.put "#{i}", "Hello #{i}" for i in [100..200]
    batch.writeSync()
    assert.equal "Hello #{i}", db.get "#{i}" for i in [100..200]

  afterEach (done) ->
    db = null
    iterator = null
    leveldb.destroy filename, done

  describe 'async', ->

    beforeEach (done) ->
      db.iterator (err, iter) ->
        iterator = iter
        done err

    it 'should get values', (done) ->
      iterator.first (err) ->
        assert.ifError err
        i = 100
        next = (err) ->
          assert.ifError err
          iterator.current (err, key, val) ->
            assert.ifError err
            expectKey = "#{i}"
            expectVal = "Hello #{i}"
            assert.equal expectKey, key
            assert.equal expectVal, val
            iterator.key (err, key) ->
              assert.ifError err
              assert.equal expectKey, key
              iterator.value (err, val) ->
                assert.ifError err
                assert.equal expectVal, val
                iterator.next if ++i <= 200 then next else done
        next()

    it 'should not get invalid key', (done) ->
      iterator.seek '201', (err) ->
        assert.ifError err
        assert.ifError iterator.valid()
        iterator.current (err, key, val) ->
          assert.ifError key
          assert.ifError val
          done err

    it 'should get values in reverse', (done) ->
      iterator.last (err) ->
        i = 200
        next = (err) ->
          assert.ifError err
          iterator.current (err, key, val) ->
            assert.ifError err
            expectKey = "#{i}"
            expectVal = "Hello #{i}"
            assert.equal expectKey, key
            assert.equal expectVal, val
            iterator.key (err, key) ->
              assert.ifError err
              assert.equal expectKey, key
              iterator.value (err, val) ->
                assert.ifError err
                assert.equal expectVal, val
                iterator.prev if --i >= 100 then next else done
        next()

  describe 'sync', ->

    beforeEach ->
      iterator = db.iterator()

    it 'should get values', ->
      iterator.firstSync()
      for i in [100..200]
        [key, val] = iterator.current()
        expectKey = "#{i}"
        expectVal = "Hello #{i}"
        assert iterator.valid()
        assert.equal expectKey, key
        assert.equal expectVal, val
        assert.equal expectKey, iterator.key()
        assert.equal expectVal, iterator.value()
        iterator.nextSync()

    it 'should not get invalid key', ->
      iterator.seekSync '201'
      assert.ifError iterator.valid()
      assert.ifError iterator.key()
      assert.ifError iterator.value()

    it 'should get values in reverse', ->
      iterator.lastSync()
      for i in [200..100]
        [key, val] = iterator.current()
        expectKey = "#{i}"
        expectVal = "Hello #{i}"
        assert iterator.valid()
        assert.equal expectKey, key
        assert.equal expectVal, val
        assert.equal expectKey, iterator.key()
        assert.equal expectVal, iterator.value()
        iterator.prevSync()

  describe 'forRange()', ->

    it 'should iterate over all keys', (done) ->
      iterator = db.iterator()
      i = 100
      iterator.forRange (err, key, val) ->
        assert.ifError
        assert.equal "#{i}", key
        assert.equal "Hello #{i}", val
        done() if ++i > 200

    it 'should iterate from start key', (done) ->
      iterator = db.iterator()
      i = 110
      iterator.forRange '110', (err, key, val) ->
        assert.ifError
        assert.equal "#{i}", key
        assert.equal "Hello #{i}", val
        done() if ++i > 200

    it 'should iterate until limit key', (done) ->
      iterator = db.iterator()
      i = 100
      iterator.forRange null, '190', (err, key, val) ->
        assert.ifError
        assert.equal "#{i}", key
        assert.equal "Hello #{i}", val
        done() if ++i > 190

    it 'should iterate over range', (done) ->
      iterator = db.iterator()
      i = 110
      iterator.forRange '110', '190', (err, key, val) ->
        assert.ifError
        assert.equal "#{i}", key
        assert.equal "Hello #{i}", val
        done() if ++i > 190
