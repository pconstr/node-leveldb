assert = require 'assert'
fs     = require 'fs'


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


module.exports = assert

