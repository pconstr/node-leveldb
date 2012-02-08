binding = require '../../build/leveldb.node'
{Batch} = require './batch'
{Iterator} = require './iterator'

noop = (err) -> throw err if err

###

    Open a leveldb database.

    @param {String} path The path to the database file.
    @param {Object} [options] Optional options corresponding to the leveldb
      options. See also the `leveldb/options.h` header file.
      @param {Boolean} [options.create_if_missing=false] If true, the
        database will be created if it is missing.
      @param {Boolean} [options.error_if_exists=false] If true, an error is
        raised if the database already exists.
      @param {Boolean} [options.paranoid_checks=false] If true, the
        implementation will do aggressive checking of the data it is
        processing and will stop early if it detects any errors.  This may
        have unforeseen ramifications: for example, a corruption of one DB
        entry may cause a large number of entries to become unreadable or
        for the entire DB to become unopenable.
      @param {Integer} [options.write_buffer_size=4*1024*1024] Amount of
        data to build up in memory (backed by an unsorted log on disk)
        before converting to a sorted on-disk file, in bytes.
      @param {Integer} [options.max_open_files=1000] Maximum number of open
        files that can be used by the database. You may need to increase
        this if your database has a large working set (budget one open file
        per 2MB of working set).
      @param {Integer} [options.block_size=4096] Approximate size of user
        data packed per block, in bytes. Note that the block size specified
        here corresponds to uncompressed data. The actual size of the unit
        read from disk may be smaller if compression is enabled. This
        parameter can be changed dynamically.
      @param {Integer} [options.block_restart_interval=16] Number of keys
        between restart points for delta encoding of keys. This parameter
        can be changed dynamically. Most clients should leave this parameter
        alone.
      @param {Boolean} [options.compression=true] Set to false to disable
        Snappy compression.
    @param {Function} callback The callback function.
      @param {Error} error The error value on error, null otherwise.
      @param {leveldb.Handle} handle If successful, the database handle.

###

exports.open = (path, options, callback) ->
  if typeof options is 'function'
    callback = options
    options = null
  throw new Error 'Missing callback' unless callback
  binding.open path, options, (err, self) ->
    return callback err if err
    callback null, new Handle self


###

    Open a leveldb database synchronously. See `leveldb.open()`.

###

exports.openSync = (path, options) ->
  new Handle binding.open path, options


###

    Destroy a leveldb database.

    @param {String} path The path to the database file.
    @param {Object} [options] Optional options. See `leveldb.open()`.
    @param {Function} [callback] Optional callback.
      @param {Error} error The error value on error, null otherwise.

###

exports.destroy = (path, options, callback) ->
  if typeof options is 'function'
    callback = options
    options = null
  binding.destroy path, options, callback or noop


###

    Destroy a leveldb database synchronously. See `leveldb.open()`.

###

exports.destroySync = (path, options) ->
  binding.destroy path, options


###

    Repair a leveldb database.

    @param {String} path The path to the database file.
    @param {Object} [options] Optional options. See `leveldb.open()`.
    @param {Function} [callback] Optional callback.
      @param {Error} error The error value on error, null otherwise.

###

exports.repair = (path, options, callback) ->
  if typeof options is 'function'
    callback = options
    options = null
  binding.repair path, options, callback or noop


###

    Repair a leveldb database synchronously. See `leveldb.repair()`.

###

exports.repairSync = (path, options) ->
  binding.repair path, options


###

    A handle represents an open leveldb database.

###

class Handle


  ###

      Constructor.

      @param {Native} self The native handle binding.

  ###

  constructor: (@self) ->


  ###

      Get a value from the database.

      @param {String|Buffer} key The key to get.
      @param {Object} [options] Optional options. See also the
        `leveldb/options.h` header file.
        @param {Boolean} [options.verify_checksums=false] If true, all data
          read from underlying storage will be verified against
          corresponding checksums.
        @param {Boolean} [options.fill_cache=true] If true, data read from
          disk will be cached in memory.
        @param {Boolean} [options.as_buffer=false] If true, data will be
          returned as a `Buffer`.
      @param {Function} callback The callback.
        @param {Error} error The error value on error, null otherwise.
        @param {String|Buffer} value If successful, the value.

  ###

  get: (key, options, callback) ->
    key = new Buffer key unless Buffer.isBuffer key
    if typeof options is 'function'
      callback = options
      options = null
    throw new Error 'Missing callback' unless callback
    @self.get key, options, callback or noop
    @


  ###

      Get a value from the database synchronously. See `Handle.get()`.

  ###

  getSync: (key, options) ->
    key = new Buffer key unless Buffer.isBuffer key
    @self.get key, options


  ###

      Put a key-value pair in the database.

      @param {String|Buffer} key The key to put.
      @param {String|Buffer} val The value to put.
      @param {Object} [options] Optional options. See `Handle.write()`.
      @param {Function} [callback] Optional callback.
        @param {Error} error The error value on error, null otherwise.

  ###

  put: (key, val, options, callback) ->
    @batch().put(key, val).write options, callback
    @


  ###

      Put a value in the database synchronously. See `Handle.put()`.

  ###

  putSync: (key, val, options) ->
    @batch().put(key, val).writeSync options
    @


  ###

      Delete a key-value pair from the database.

      @param {String|Buffer} key The key to put.
      @param {Object} [options] Optional options. See `Handle.write()`.
      @param {Function} [callback] Optional callback.
        @param {Error} error The error value on error, null otherwise.

  ###

  del: (key, options, callback) ->
    @batch().del(key).write options, callback
    @


  ###

      Delete a value from the database synchronously. See `Handle.del()`.

  ###

  delSync: (key, options) ->
    @batch().del(key).writeSync options
    @


  ###

      Commit batch operations to disk.

      @param {leveldb.Batch} batch The batch object.
      @param {Object} [options] Optional options. See also the
        `leveldb/options.h` header file.
        @param {Boolean} [options.sync=false] If true, the write will be
          flushed from the operating system buffer cache (by calling
          WritableFile::Sync()) before the write is considered complete. If
          this flag is true, writes will be slower.
      @param {Function} [callback] Optional callback.
        @param {Error} error The error value on error, null otherwise.

  ###

  write: (batch, options, callback) ->
    if typeof options is 'function'
      callback = options
      options = null
    @self.write batch.self, options, callback or noop
    @


  ###

      Commit batch operations to disk synchronously. See `Handle.write()`.

  ###

  writeSync: (batch, options) ->
    @self.write batch.self, options
    @


  ###

      Create a new batch object supporting `Batch.write()` using this
      database handle.

  ###

  batch: ->
    new Batch @self


  ###

      Create a new iterator.

  ###

  iterator: ->
    new Iterator @self.iterator()


  ###

      Create a new snapshot.

  ###

  snapshot: ->
    new Snapshot @, @self.snapshot()


  ###

      Get a database property.

      @param {String} name The database property name. See the
        `leveldb/db.h` header file for property names.

  ###

  property: (name) ->
    @self.property(name)

  # helper function for approximateSizes()
  # normalizes arguments to a flattened array of
  #   [ key1, val1, ..., keyN, valN]
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


  ###

      Approximate the on-disk storage bytes for key ranges.

      Usage:

        db.approximateSize('foo', 'bar', callback);
        db.approximateSize(['foo', 'bar'], ['baz', 'zee'], callback);
        db.approximateSize([['foo', 'bar'], ['baz', 'zee']], callback);

      @param {Array} slices An array of start/limit key ranges.
        @param {String|Buffer} start The start key in the range, inclusive.
        @param {String|Buffer} limit The limit key in the range, exclusive.

  ###

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


###

    A Snapshot represents the state of the database when the snapshot was
    created. Get operations return the value of the key when the snapshot
    was created.

###

class Snapshot


  ###

      Constructor.

      @param {leveldb.Handle} self The database handle from which this
        snapshot was created.
      @param {Native} snapshot The native snapshot object.

  ###

  constructor: (@self, @snapshot) ->


  ###

      Get a value from the database snapshot. See `Handle.get()`.

  ###

  get: (key, options = {}, callback) ->
    if typeof options is 'function'
      callback = options
      options = {}
    options.snapshot = @snapshot
    @self.get key, options, callback or noop
    @


  ###

      Get a value from the database snapshot synchronously.
      See `Handle.get()`.

  ###

  getSync: (key, options = {}) ->
    options.snapshot = @snapshot
    @self.get key, options
