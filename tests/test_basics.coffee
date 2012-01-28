assert   = require './asserts'
leveldb  = require '../lib'
fs       = require 'fs'


describe 'LevelDB Database level Operations', ->
   beforeEach (done) ->
      @path = "/tmp/test-database"
      done()

   afterEach (done) ->
      if @db
         leveldb.DB.destroyDB(@path, {})
         assert.fileDoesNotExist(@path)
      done()

   it 'create delete database', (done) ->
      @db = new leveldb.DB()

      @db.open @path, {create_if_missing: true}, (err) =>
         assert.ok(not err, err)
         assert.fileExists @path

         # close the database
         @db.close()

         # make sure it cleaned up correctly.
         done()

   it 'can not create if database already exists', (done) ->
      assert.fileDoesNotExist @path
      fh = fs.openSync(@path, "w+")
      assert.ok fh
      fs.writeSync(fh, "test data")
      fs.closeSync(fh)

      db = new leveldb.DB()

      db.open @path, {}, (err) =>
         assert.ok(err, "Since something exists at path the database should not be created.")
         fs.unlinkSync(@path)
         done()

