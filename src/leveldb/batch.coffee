binding = require '../../build/leveldb.node'

exports.Batch = class Batch

  constructor: ->
    @self = new binding.Batch

  put: (key, val) ->
    key = new Buffer key unless Buffer.isBuffer key
    val = new Buffer val unless Buffer.isBuffer val
    @self.put key, val

  del: (key, val) ->
    key = new Buffer key unless Buffer.isBuffer key
    @self.del key

  clear: ->
    @self.clear()
