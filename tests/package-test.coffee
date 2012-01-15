assert  = require 'assert'
fs      = require 'fs'
leveldb = require '../lib'





describe 'package', ->
  package = JSON.parse fs.readFileSync __dirname + '/../package.json', 'utf8'
  bindingVersion = '1.2'

  it 'should have version', ->
    assert.equal package.version, leveldb.version

  it 'should have read-only binding version', () ->
    leveldb.bindingVersion = 0
    assert.equal leveldb.bindingVersion, bindingVersion
    delete leveldb.bindingVersion
    assert.equal leveldb.bindingVersion, bindingVersion
