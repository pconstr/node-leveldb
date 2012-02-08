binding = require '../../build/leveldb.node'

exports.Iterator = class Iterator

  noop = (err) ->
    throw err if err

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

    if callback
      [ key, val ] = @self.current options
      @self.next (err) => callback err, key, val
    else
      @self.next noop

  nextSync: ->
    current = @current()
    @self.next()
    current

  prev: (callback) ->
    if typeof options is 'function'
      callback = options
      options = null

    if callback
      [ key, val ] = @self.current options
      @self.prev (err) => callback err, key, val
    else
      @self.prev noop

  prevSync: ->
    current = @current()
    @self.prev()
    current

  key: (options) ->
    @self.key options

  value: (options) ->
    @self.value options

  current: (options) ->
    @self.current()
