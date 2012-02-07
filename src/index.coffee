leveldb = exports = module.exports = require './leveldb/handle'
binding = require '../build/leveldb.node'

leveldb.version = '0.5.0'
leveldb.bindingVersion = binding.bindingVersion

leveldb.Batch = require('./leveldb/batch').Batch
