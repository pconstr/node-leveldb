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

exports.Handle = class Handle

  constructor: (@self) ->

  valid: ->
    @self.valid()

  get: (key, options, callback) ->
    key = new Buffer key unless Buffer.isBuffer key
    if typeof options is 'function'
      callback = options
      options = null
    @self.get key, options, callback or noop

  getSync: (key, options) ->
    key = new Buffer key unless Buffer.isBuffer key
    @self.get key, options

  put: (key, val, options, callback) ->
    batch = new Batch
    batch.put key, val
    @write batch, options, callback

  putSync: (key, val, options) ->
    batch = new Batch
    batch.put key, val
    @writeSync batch, options

  del: (key, options, callback) ->
    batch = new Batch
    batch.del key
    @write batch, options, callback

  delSync: (key, options) ->
    batch = new Batch
    batch.del key
    @write batch, options

  write: (batch, options, callback) ->
    if typeof options is 'function'
      callback = options
      options = null
    @self.write batch.self, options, callback or noop

  writeSync: (batch, options) ->
    @self.write batch.self, options

  iterator: (options, callback) ->
    if typeof options is 'function'
      callback = options
      options = null

    throw 'Missing callback' unless callback

    it = new Iterator @self.iterator options
    it.first (err) -> callback err, it

  iteratorSync: (options) ->
    it = new Iterator @self.iterator options
    it.firstSync()
    it

  snapshot: (options) ->
    new Snapshot @, @self.snapshot options

  property: (name) ->
    @self.property(name)

  approximateSizes: (bounds) ->
    bounds = [arguments] unless Array.isArray bounds
    slices = for [ key, val ] in bounds when key and val
      [
        if Buffer.isBuffer key then key else new Buffer key
        if Buffer.isBuffer val then val else new Buffer val
      ]
    @self.approximateSizes slices

  # TODO: compactRange

class Snapshot

  constructor: (@self, @snapshot) ->

  get: (key, options = {}, callback) ->
    if typeof options is 'function'
      callback = options
      options = {}
    options.snapshot = @snapshot
    @self.get key, options, callback or noop

  getSync: (key, options = {}) ->
    options.snapshot = @snapshot
    @self.get key, options

  valid: ->
    @self.valid()
