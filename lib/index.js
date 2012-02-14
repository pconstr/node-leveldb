var binding, exports, leveldb;

leveldb = exports = module.exports = require('./leveldb/handle');

binding = require('./leveldb.node');

leveldb.version = '0.6.0';

leveldb.bindingVersion = binding.bindingVersion;

leveldb.Batch = require('./leveldb/batch').Batch;
