binding = require '../../build/leveldb.node'

exports.Iterator = class Iterator

  noop = (err) -> throw err if err

  constructor: (@self) ->

  valid: ->
    @self.valid()

  seek: (key, callback) ->
    key = new Buffer key unless Buffer.isBuffer key
    @self.seek key, callback or noop

  seekSync: (key) ->
    key = new Buffer key unless Buffer.isBuffer key
    @self.seek key

  first: (callback) ->
    @self.first callback or noop

  firstSync: ->
    @self.first()

  last: (callback) ->
    @self.last callback or noop

  lastSync: ->
    @self.last()

  next: (options, callback) ->
    if typeof options is 'function'
      callback = options
      options = null
    @self.next callback or noop

  nextSync: ->
    @self.next()

  prev: (callback) ->
    if typeof options is 'function'
      callback = options
      options = null
    @self.prev callback or noop

  prevSync: ->
    @self.prev()

  key: (options) ->
    @self.key options

  value: (options) ->
    @self.value options

  current: (options) ->
    @self.current()
