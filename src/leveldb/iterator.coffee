binding = require '../../build/leveldb.node'

exports.Iterator = class Iterator

  noop = (err) -> throw err if err

  constructor: (@self) ->

  valid: ->
    @self.valid()

  seek: (key, callback = noop) ->
    key = new Buffer key unless Buffer.isBuffer key
    @self.seek key, callback

  seekSync: (key) ->
    key = new Buffer key unless Buffer.isBuffer key
    @self.seek key

  first: (callback = noop) ->
    @self.first callback

  firstSync: ->
    @self.first()

  last: (callback = noop) ->
    @self.last callback

  lastSync: ->
    @self.last()

  next: (callback = noop) ->
    @self.next callback

  nextSync: ->
    @self.next()

  prev: (callback = noop) ->
    @self.prev callback

  prevSync: ->
    @self.prev()

  key: (options) ->
    key = @self.key options
    key = key.toString 'utf8' unless options.as_buffer
    key

  value: (options) ->
    val = @self.value options
    val = val.toString 'utf8' unless options.as_buffer
    val

  current: (options = {}) ->
    [ key, val ] = @self.current()
    val = val.toString 'utf8' unless options.as_buffer
    key = key.toString 'utf8' unless options.as_buffer
    [ key, val ]

