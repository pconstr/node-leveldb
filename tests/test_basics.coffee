assert  = require 'assert'
leveldb = require '../lib'
fs      = require 'fs'
testCase = require('nodeunit').testCase

#{ File Assertions
_file_exists = (path) ->
   has_file = true
   try
      fs.statSync path
   catch ex
      assert.equal(ex.code, 'ENOENT', ex)
      has_file = false

   return has_file
   
assert.fileExists = (path) ->
   assert.ok(_file_exists(path), "File should exist: " + path)
assert.fileDoesNotExist = (path) ->
   assert.ok(not _file_exists(path), 'File should not exist: ' + path)
#}


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

