binding = require '../leveldb.node'

###

    An iterator allows sequential and random access to the database.

    Usage:

        var leveldb = require('leveldb');

        leveldb.open('/tmp/test.db', function(err, db) {
          db.iterator(function(err, it) {

            // iterator is initially invalid
            it.first(function(err) {

              // get a key
              it.get('foobar', function(err, val) {

                console.log(val);

              });
            });
          });
        });

###

exports.Iterator = class Iterator

  busy = false
  validKey = false

  lock = ->
    throw new Error 'Concurrent operations not supported' if busy
    busy = true

  unlock = ->
    throw new Error 'Not locked' unless busy
    busy = false

  afterSeek = (callback) -> (err, valid) ->
    unlock()
    validKey = valid
    callback err if callback

  ###

      Constructor.

      @param {Native} self The underlying iterator object.

  ###

  constructor: (@self) ->

  ###

      Apply a callback over a range.

      The iterator will be positioned at the given key or the first key if
      not given, then the callback will be applied on each record moving
      forward until the iterator is positioned at the limit key or at an
      invalid key. Stops on first error.

      @param {String|Buffer} [startKey] Optional start key (inclusive) from
        which to begin applying the callback. If not given, defaults to the
        first key.
      @param {String|Buffer} [limitKey] Optional limit key (inclusive) at
        which to end applying the callback.
      @param {Object} [options] Optional options.
        @param {Boolean} [options.as_buffer=false] If true, data will be
          returned as a `Buffer`.
      @param {Function} callback The callback to apply to the range.
        @param {Error} error The error value on error, null otherwise.
        @param {String|Buffer} key The key.
        @param {String|Buffer} value The value.

  ###

  forRange: ->

    args = Array.prototype.slice.call arguments

    # required callback
    callback = args.pop()
    throw new Error 'Missing callback' unless callback

    # optional options
    options = args[args.length - 1]
    if typeof options is 'object' and not Buffer.isBuffer options
      args.pop()
    else
      options = null

    # optional keys
    [ startKey, limitKey ] = args

    limit = limitKey.toString 'binary' if limitKey

    next = (err) =>
      return callback err if err
      @current options, (err, key, val) =>
        return callback err if err
        if key
          callback null, key, val
          @next next if not limit or limit isnt key.toString 'binary'

    if startKey
      @seek startKey, next
    else
      @first next


  ###

      True if the iterator is positioned at a valid key.

  ###

  valid: -> validKey


  ###

      Position the iterator at a key.

      @param {String} key The key at which to position the iterator.
      @param {Function} [callback] Optional callback.
        @param {Error} error The error value on error, null otherwise.

  ###

  seek: (key, callback) ->
    key = new Buffer key unless Buffer.isBuffer key
    lock()
    @self.seek key, afterSeek callback


  ###

      Position the iterator at the first key.

      @param {Function} [callback] Optional callback.
        @param {Error} error The error value on error, null otherwise.

  ###

  first: (callback) ->
    lock()
    @self.first afterSeek callback


  ###

      Position the iterator at the last key.

      @param {Function} [callback] Optional callback.
        @param {Error} error The error value on error, null otherwise.

  ###

  last: (callback) ->
    lock()
    @self.last afterSeek callback


  ###

      Advance the iterator to the next key.

      @param {Function} [callback] Optional callback.
        @param {Error} error The error value on error, null otherwise.

  ###

  next: (callback) ->
    throw new Error 'Illegal state' unless validKey
    lock()
    @self.next afterSeek callback


  ###

      Advance the iterator to the previous key.

      @param {Function} [callback] Optional callback.
        @param {Error} error The error value on error, null otherwise.

  ###

  prev: (callback) ->
    throw new Error 'Illegal state' unless validKey
    lock()
    @self.prev afterSeek callback


  ###

      Get the key at the current iterator position.

      @param {Object} [options] Optional options.
        @param {Boolean} [options.as_buffer=false] If true, data will be
          returned as a `Buffer`.
      @param {Function} callback The callback function.
        @param {Error} error The error value on error, null otherwise.
        @param {String|Buffer} key The key if successful.

  ###

  key: (options = {}, callback) ->
    throw new Error 'Illegal state' unless validKey

    # optional options
    if typeof options is 'function'
      callback = options
      options = {}

    throw new Error 'Missing callback' unless callback

    return callback() unless validKey

    lock()
    @self.key (err, key) ->
      unlock()
      key = key.toString 'utf8' if key and not options.as_buffer
      callback err, key


  ###

      Get the value at the current iterator position.

      @param {Object} [options] Optional options.
        @param {Boolean} [options.as_buffer=false] If true, data will be
          returned as a `Buffer`.
      @param {Function} callback The callback function.
        @param {Error} error The error value on error, null otherwise.
        @param {String|Buffer} value The value if successful.

  ###

  value: (options = {}, callback) ->

    # optional options
    if typeof options is 'function'
      callback = options
      options = {}

    @current options, (err, key, val) -> callback err, val


  ###

      Get the key and value at the current iterator position.

      @param {Object} [options] Optional options.
        @param {Boolean} [options.as_buffer=false] If true, data will be
          returned as a `Buffer`.
      @param {Function} callback The callback function.
        @param {Error} error The error value on error, null otherwise.
        @param {String|Buffer} key The key if successful.
        @param {String|Buffer} value The value if successful.

  ###

  current: (options = {}, callback) ->
    # optional options
    if typeof options is 'function'
      callback = options
      options = {}

    throw new Error 'Missing callback' unless callback

    return callback() unless validKey

    lock()
    @self.current (err, kv) ->
      unlock()
      if kv
        [ key, val ] = kv
        unless options.as_buffer
          key = key.toString 'utf8' if key
          val = val.toString 'utf8' if val
      callback err, key, val
