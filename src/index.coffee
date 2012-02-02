leveldb = exports = module.exports = require './leveldb/handle'

leveldb.version = '0.5.0'

leveldb.binding = binding = require '../build/leveldb.node'

leveldb.Batch = binding.Batch
