assert   = require './asserts'
leveldb  = require '../lib'
fs       = require 'fs'
testCase = require('nodeunit').testCase


module.exports = testCase({
   setUp: (done) ->
      @path = "/tmp/test-database"
      done()

   tearDown: (done) ->
      if @db
         leveldb.DB.destroyDB(@path, {})
         assert.fileDoesNotExist(@path)
      done()

   'create delete database': (test) ->
      @db = new leveldb.DB()

      @db.open @path, {create_if_missing: true}, (err) =>
         assert.ok(not err, err)
         assert.fileExists @path

         # close the database
         @db.close()

         # make sure it cleaned up correctly.
         test.done()

   'can not create if database already exists': (test) ->
      assert.fileDoesNotExist @path
      fh = fs.openSync(@path, "w+")
      assert.ok fh
      fs.writeSync(fh, "test data")
      fs.closeSync(fh)

      db = new leveldb.DB()

      db.open @path, {}, (err) =>
         assert.ok(err, "Since something exists at path the database should not be created.")
         fs.unlinkSync(@path)
         test.done()
})

