assert  = require 'assert'
leveldb = require '../lib'
fs      = require 'fs'

exports['create delete database'] = (test) ->
   path = "/tmp/test-database"
   db = new leveldb.DB()
   db.open path, {create_if_missing: true}, (err) =>
      # make sure the database opened correctly.
      assert.ok(not err)

      # make sure the database exits now, this will except otherwise
      fs.statSync(path)

      # close the database
      db.close()

      # clean up the disk space
      leveldb.DB.destroyDB(path, {})

      # make sure it cleaned up correctly.
      no_file = false
      try
         fs.statSync path
      catch ex
         # The file did not exist
         assert.equal(ex.code, 'ENOENT')
         no_file = true

      assert.ok no_file

      test.done()

