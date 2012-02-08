binding = require '../../build/leveldb.node'
{Batch} = require './batch'
{Iterator} = require './iterator'

noop = ->

exports.open = (path, options, callback) ->
  if typeof options is 'function'
    callback = options
    options = null
  callback ?= noop
  binding.open path, options, (err, self) ->
    return callback err if err
    callback null, new Handle self

exports.openSync = (path, options) ->
  new Handle binding.open path, options

exports.destroy = (path, options, callback) ->
  if typeof options is 'function'
    callback = options
    options = null
  binding.destroy path, options, callback or noop

exports.destroySync = (path, options) ->
  binding.destroy path, options

exports.repair = (path, options, callback) ->
  if typeof options is 'function'
    callback = options
    options = null
  binding.repair path, options, callback or noop

exports.repairSync = (path, options) ->
  binding.repair path, options

class Handle



  get: (key, options, callback) ->
    key = new Buffer key unless Buffer.isBuffer key
    if typeof options is 'function'
      callback = options
      options = null
    @self.get key, options, callback or noop
    @

  getSync: (key, options) ->
    key = new Buffer key unless Buffer.isBuffer key
    @self.get key, options

  put: (key, val, options, callback) ->
    @batch().put(key, val).write options, callback
    @

  putSync: (key, val, options) ->
    @batch().put(key, val).writeSync options
    @

  del: (key, options, callback) ->
    @batch().del(key).write options, callback
    @

  delSync: (key, options) ->
    @batch().del(key).writeSync options
    @

  write: (batch, options, callback) ->
    if typeof options is 'function'
      callback = options
      options = null
    @self.write batch.self, options, callback or noop
    @

  writeSync: (batch, options) ->
    @self.write batch.self, options
    @

  batch: ->
    new Batch @self

  iterator: (options) ->
    new Iterator @self.iterator options

  snapshot: (options) ->
    new Snapshot @, @self.snapshot options

  property: (name) ->
    @self.property(name)

  unpackSlices = (args) ->
    bounds =
      if Array.isArray args[0]
        if Array.isArray args[1]
          Array.prototype.slice.call args
        else
          args[0]
      else
        [args]

    slices = []

    for [ key, val ] in bounds when key and val
      slices.push if Buffer.isBuffer key then key else new Buffer key
      slices.push if Buffer.isBuffer val then val else new Buffer val

    slices

  approximateSizes: ->
    args = Array.prototype.slice.call arguments
    callback =
      if typeof args[args.length - 1] is 'function'
        args.pop()
      else
        noop
    @self.approximateSizes unpackSlices(args), callback

  approximateSizesSync: ->
    @self.approximateSizes unpackSlices arguments

  # TODO: compactRange

class Snapshot

  constructor: (@self, @snapshot) ->

  get: (key, options = {}, callback) ->
    if typeof options is 'function'
      callback = options
      options = {}
    options.snapshot = @snapshot
    @self.get key, options, callback or noop
    @

  getSync: (key, options = {}) ->
    options.snapshot = @snapshot
    @self.get key, options
