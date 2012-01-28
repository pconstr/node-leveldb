assert   = require './asserts'
leveldb  = require '../lib'
defer    = require "twisted-deferred"


describe 'LevelDB Common Operations', ->
   beforeEach (done) ->
      @path = "/tmp/test-database"
      @db   = new leveldb.DB()

      @db.open = @db.open.bind(@db)
      @db.put  = @db.put.bind(@db)
      @db.get  = @db.get.bind(@db)

      @db.open.bind(@db) @path, {create_if_missing: true}, (err) =>
         assert.ok not err, err
         done()

   afterEach () ->
      leveldb.DB.destroyDB(@path, {})

   it 'can get and put data', (done) ->
      # put two keys and then ensure that we can each correctly
      d1 = defer.toDeferred @db.put, "1", "the data"
      d2 = defer.toDeferred @db.put, "2", "more data"

      d = defer.DeferredList([d1, d2])

      assert_val = (res, key, value) =>
         get_defer = defer.toDeferred(@db.get, key)
         get_defer.addCallback (foundVal) ->
            assert.equal value, foundVal
            res
         get_defer

      d.addCallback(assert_val, "1", "the data")
      d.addCallback(assert_val, "2", "more data")

      d.addErrback (err) ->
         console.log err.message.stack
         err
      d.addCallback () ->
         done()

   it 'can iterate over keys', (done) ->
      # put several keys into the database and then iterate over them
      d1 = defer.toDeferred @db.put, "1", "the data"
      d2 = defer.toDeferred @db.put, "2", "more data"

      d = defer.DeferredList([d1, d2])

      d.addCallback () =>
         d3 = new defer.Deferred()
         iterator = @db.newIterator({})
         keys = []

         iterator.seekToFirst () ->
            while iterator.valid()
               key = iterator.key().toString('utf8')
               keys.push(key)
               iterator.next()
            d3.callback()

            assert.equal(keys[0], "1")
            assert.equal(keys[1], "2")

         return d3

      d.addErrback (err) ->
         console.log err.message.stack
         err
      d.addCallback () ->
         done()

