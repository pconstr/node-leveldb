assert  = require 'assert'
crypto  = require 'crypto'
leveldb = require '../lib'
path    = require 'path'




describe 'Handle', ->
  filename = "#{__dirname}/../tmp/handle-test-file"
  db = null

  itShouldBehave = (test) ->

    describe 'with ascii values', ->
      key = "Hello"
      val = "World"
      test key, val
      describe 'as_buffer', -> test key, val, true

    describe 'with buffer values', ->
      key = new Buffer [1,9,9,9]
      val = new Buffer [1,2,3,4]
      test key, val
      describe 'as_buffer', -> test key, val, true

  describe 'async', ->

    beforeEach (done) ->
      leveldb.open filename,
        create_if_missing: true, error_if_exists: true, (err, handle) ->
          assert.ifError err
          db = handle
          done()

    afterEach (done) ->
      db = null
      iterator = null
      leveldb.destroy filename, done

    it 'should repair database', (done) ->
      leveldb.repair filename, (err) ->
        assert.ifError err
        assert path.existsSync filename
        done()

    it 'should get property', (done) ->
      db.property 'leveldb.stats', (err, value) ->
        assert.ifError err
        assert value
        done err

    it 'should not property', (done) ->
      db.property '', (err, value) ->
        assert.ifError value
        done err

    it 'should get approximate size of 0', (done) ->
      db.approximateSizes ['0', '1'], (err, sizes) ->
        assert.ifError err
        assert.equal 1, sizes.length
        assert.ifError sizes[0]

        db.approximateSizes [['0', '1']], (err, sizes) ->
          assert.ifError err
          assert.equal 1, sizes.length
          assert.ifError sizes[0]
          done()

    it 'should get approximate size of range', (done) ->
      batch = db.batch()
      batch.put "#{i}", crypto.randomBytes 1024 for i in [10..99]
      batch.writeSync sync: true

      # reopen database for accurate sizes
      db = leveldb.open filename

      db.approximateSizes ['10', '99'], (err, sizes) ->
        assert.ifError err
        assert.equal 1, sizes.length
        assert sizes[0]

        db.approximateSizes [['10', '99']], (err, sizes) ->
          assert.ifError err
          assert.equal 1, sizes.length
          assert sizes[0]

          db.approximateSizes [['10', '49'], ['50', '99']], (err, sizes) ->
            assert.ifError err
            assert.equal 2, sizes.length
            assert sizes[0]
            assert sizes[1]
            done()

    itShouldBehave (key, val, asBuffer) ->

      it 'should put key/value pair', (done) ->
        db.put key, val, (err) ->
          assert.ifError err
          db.get key, as_buffer: asBuffer, (err, value) ->
            assert.ifError err
            assert Buffer.isBuffer value if asBuffer
            assert.equal val.toString(), value.toString()
            done()

      it 'should delete key', (done) ->
        db.put key, val, (err) ->
          assert.ifError err
          db.del key, (err) ->
            assert.ifError err
            db.get key, (err, value) ->
              assert.ifError value
              done err

  describe 'sync', ->

    beforeEach ->
      db = leveldb.open filename, create_if_missing: true, error_if_exists: true

    afterEach ->
      db = null
      iterator = null
      leveldb.destroySync filename

    it 'should repair database', ->
      leveldb.repairSync filename

    it 'should get property', ->
      assert db.property 'leveldb.stats'
      assert.ifError db.property ''

    it 'should get approximate size of 0', ->
      sizes = db.approximateSizes ['0', '1']
      assert.equal 1, sizes.length
      assert.ifError sizes[0]

      sizes = db.approximateSizes [['0', '1']]
      assert.equal 1, sizes.length
      assert.ifError sizes[0]

    it 'should get approximate size of range', ->
      batch = db.batch()
      batch.put "#{i}", crypto.randomBytes 1024 for i in [10..99]
      batch.writeSync sync: true

      # reopen database for accurate sizes
      db = leveldb.open filename

      sizes = db.approximateSizes ['10', '99']
      assert.equal 1, sizes.length
      assert sizes[0]

      sizes = db.approximateSizes [['10', '99']]
      assert.equal 1, sizes.length
      assert sizes[0]

      sizes = db.approximateSizes [['10', '49'], ['50', '99']]
      assert.equal 2, sizes.length
      assert sizes[0]
      assert sizes[1]

    itShouldBehave (key, val, asBuffer) ->

      it 'should put key/value pair', ->
        db.putSync key, val
        value = db.get key, as_buffer: asBuffer
        assert Buffer.isBuffer value if asBuffer
        assert.equal val.toString(), value.toString()

      it 'should delete key', ->
        db.putSync key, val
        db.delSync key
        value = db.get key
        assert.ifError value
