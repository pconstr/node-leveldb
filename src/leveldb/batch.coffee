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

  noop = (err) -> throw err if err


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

    # to buffer if string
    key = new Buffer key unless Buffer.isBuffer key
    val = new Buffer val unless Buffer.isBuffer val

    # call native binding
    @self.put key, val

    @ # return this for chaining


  ###

      Add a delete operation to the batch.

      @param {String|Buffer} key The key to delete.

  ###

  del: (key) ->

    # to buffer if string
    key = new Buffer key unless Buffer.isBuffer key

    # call native binding
    @self.del key

    @ # return this for chaining


  ###

      Commit the batch operations to disk. See `Handle.write()`.

  ###

  write: (options, callback) ->

    # require handle
    throw new Error 'No handle' unless @handle

    # optional options
    if typeof options is 'function'
      callback = options
      options = null

    # optional callback
    callback or= noop

    # call native method
    @handle.write @self, options, (err) =>

      # clear batch
      @self.clear() unless err

      # call callback
      callback err

    # no chaining while buffer is in use async


  ###

      Commit the batch operations to disk synchronously. See `Batch.write()`.

  ###

  writeSync: (options) ->

    # require handle
    throw new Error 'No handle' unless @handle

    # call native method
    @handle.write @self, options

    # clear batch
    @self.clear()

    @ # return this for chaining


  ###

      Reset this batch instance by removing all pending operations.

  ###

  clear: ->
    @self.clear()
