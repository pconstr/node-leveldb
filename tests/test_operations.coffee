assert   = require './asserts'
leveldb  = require '../lib'
fs       = require 'fs'
testCase = require('nodeunit').testCase


module.exports = testCase({
   setUp: (done) ->
      @path = "/tmp/test-database"
      @db   = new leveldb.DB()

      @db.open @path, {create_if_missing: true}, (err) =>
         assert.ok not err, err
         done()

   'getting and putting data': (test) ->
      @db.put "1", "the data", (err) =>
         assert.ok not err, err
         @db.get "1", (err, val) =>
            assert.ok not err, err
            assert.equal val, "the data"
            test.done()

})
