binding = require '../../build/leveldb.node'

exports.Batch = class Batch

  noop = ->

  constructor: (@handle) ->
    @self = new binding.Batch

  put: (key, val) ->
    key = new Buffer key unless Buffer.isBuffer key
    val = new Buffer val unless Buffer.isBuffer val
    @self.put key, val
    @

  del: (key, val) ->
    key = new Buffer key unless Buffer.isBuffer key
    @self.del key
    @

  write: (options, callback) ->
    throw new Error 'No handle' unless @handle

    if typeof options is 'function'
      callback = options
      options = null

    @handle.write @self, options, (err) =>
      @self.clear()
      callback? err
    @

  writeSync: (options) ->
    throw new Error 'No handle' unless @handle
    @handle.write @self, options
    @self.clear()
    @

  clear: ->
    @self.clear()
