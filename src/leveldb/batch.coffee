binding = require '../../build/leveldb.node'

###

    A batch holds a sequence of put/delete operations to atomically write to
    the database.

    Usage:

        var leveldb = require('leveldb');

        var db = leveldb.openSync('/tmp/test.db')
          , batch = db.batch();

        batch
          .put('foo', 'bar')
          .del('baz')
          .put('pass', 'xyzzy')
          .writeSync();

        var db2 = leveldb.openSync('/tmp/test2.db')
          , batch2 = new leveldb.Batch;

        batch2
          .put('foo', 'coconut')
          .del('pass');

        db.writeSync(batch2);
        db2.writeSync(batch2);

        // batch2 has not been cleared to allow for reuse.
        batch2.clear();

        // works because the batch is cleared after committing when using
        // batch.commit() or batch.commitSync()
        batch
          .put('hello', 'world')
          .put('goodbye', 'world');

        db.writeSync(batch);
        db2.writeSync(batch);

###

exports.Batch = class Batch

  noop = ->


  ###

      Constructor.

      @param {Handle} [handle] Pass a database handle to use with
        `batch.write()` or `batch.writeSync()`.

  ###

  constructor: (@handle) ->
    @self = new binding.Batch


  ###

      Add a put operation to the batch.

      @param {String|Buffer} key The key to put.
      @param {String|Buffer} val The value to put.

  ###

  put: (key, val) ->
    key = new Buffer key unless Buffer.isBuffer key
    val = new Buffer val unless Buffer.isBuffer val
    @self.put key, val
    @


  ###

      Add a delete operation to the batch.

      @param {String|Buffer} key The key to delete.

  ###

  del: (key, val) ->
    key = new Buffer key unless Buffer.isBuffer key
    @self.del key
    @


  ###

      Commit the batch operations to disk.

      @param {Object} [options] Optional options. See `Handle.write()`.
      @param {Function} [callback] Optional callback.

  ###

  write: (options, callback) ->
    throw new Error 'No handle' unless @handle

    if typeof options is 'function'
      callback = options
      options = null

    @handle.write @self, options, (err) =>
      @self.clear()
      callback? err
    @


  ###

      Commit the batch operations to disk synchronously. See `Batch.write()`.

  ###

  writeSync: (options) ->
    throw new Error 'No handle' unless @handle
    @handle.write @self, options
    @self.clear()
    @


  ###

      Reset this batch instance by removing all pending operations.

  ###

  clear: ->
    @self.clear()
