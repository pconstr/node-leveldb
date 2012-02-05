binding = require '../../build/leveldb.node'

exports.Iterator = class Iterator

  noop = ->

  constructor: (@self) ->

  valid: ->
    @self.valid()

  seek: (key, callback) ->
    @self.seek key, if callback then => callback @error() else noop

  seekSync: (key) ->
    @self.seek key
    throw err if err = @error()

  first: (callback) ->
    @self.first if callback then => callback @error() else noop

  firstSync: ->
    @self.first()
    throw err if err = @error()

  last: (callback) ->
    @self.last if callback then => callback @error() else noop

  lastSync: ->
    @self.last()
    throw err if err = @error()

  next: (callback) ->
    if @self.valid()
      kv = @current() if callback
      @self.next if callback then => callback @error(), kv[0], kv[1] else noop
    else if callback
      callback @error()

  nextSync: ->
    if @self.valid()
      current = @current()
      @self.next()
      throw err if err = @error()
      current
    else
      throw @error()

  prev: (callback) ->
    if @self.valid()
      kv = @current() if callback
      @self.prev if callback then => callback @error(), kv[0], kv[1] else noop
    else if callback
      callback @error()

  prevSync: ->
    if @self.valid()
      current = @current()
      @self.prev()
      throw err if err = @error()
      current
    else
      throw @error()

  current: (options) ->
    if @self.valid()
      key = @self.key options
      val = @self.value options
      [key, val]

  error: ->
    if (err = @self.status()) is 'OK' then null else err
