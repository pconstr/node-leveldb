leveldb = exports = module.exports = require './leveldb/handle'
leveldb.binding = binding = require '../build/leveldb.node'

leveldb.version = '0.5.0'
leveldb.bindingVersion = binding.bindingVersion

leveldb.Batch = binding.Batch
