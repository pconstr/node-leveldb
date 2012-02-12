binding = require '../leveldb.node'

exports.Iterator = class Iterator

  noop = (err) -> throw err if err

  constructor: (@self) ->

  # startKey, [limitKey], [options], callback
  forRange: (startKey) ->

    args = Array.prototype.slice.call args, 1

    limitKey = args.shift() if typeof args[0] is 'string' or Buffer.isBuffer args[0]
    options = args.shift() if typeof args[0] is 'object'

    # required callback
    throw new Error 'Missing callback' unless typeof args[0] is 'function'
    callback = args[0]

    limit = limitKey.toString 'binary' if limitKey

    next = (err) =>
      return callback err if err
      @current options, (err, key, val) =>
        callback err, key, val
        if not err and key and limit and key.toString 'binary' isnt limit
          @next next

    @seek startKey, next

  forEach: (options, callback) ->

    # optional options
    if typeof options is 'function'
      callback = options
      options = {}

    # required callback
    throw new Error 'Missing callback' unless callback

    next = (err) =>
      return callback err if err
      @current options, (err, key, val) =>
        callback err, key, val
        @next next unless err

    @first next

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

  key: (options = {}, callback) ->

    # optional options
    if typeof options is 'function'
      callback = options
      options = {}

    if callback

      # async
      @self.key (err, key) ->
        key = key.toString 'utf8' unless err or options.as_buffer
        callback err, key

    else

      # sync
      key = @self.key()
      key = key.toString 'utf8' if key and not options.as_buffer
      key

  value: (options = {}, callback) ->

    # optional options
    if typeof options is 'function'
      callback = options
      options = {}

    if callback

      # async
      @self.value (err, val) ->
        val = val.toString 'utf8' unless err or options.as_buffer
        callback err, val

    else

      # sync
      val = @self.value()
      val = val.toString 'utf8' if val and not options.as_buffer
      val

  current: (options = {}, callback) ->

    # optional options
    if typeof options is 'function'
      callback = options
      options = {}

    if callback

      # async
      @self.current (err, kv) ->
        if kv
          [ key, val ] = kv
          unless err or options.as_buffer
            key = key.toString 'utf8'
            val = val.toString 'utf8'
        callback err, key, val

    else

      # sync
      if kv = @self.current()
        [ key, val ] = kv
        unless options.as_buffer
          val = val.toString 'utf8'
          key = key.toString 'utf8'
      [ key, val ]
