var Batch, binding;

binding = require('../leveldb.node');

/*

    A batch holds a sequence of put/delete operations to atomically write to
    the database.

    Usage:

        var leveldb = require('leveldb');

        var db = leveldb.open('/tmp/test.db')
          , batch = db.batch();

        batch
          .put('foo', 'bar')
          .del('baz')
          .put('pass', 'xyzzy')
          .writeSync();

        var db2 = leveldb.open('/tmp/test2.db')
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
*/

exports.Batch = Batch = (function() {
  var noop;

  noop = function(err) {
    if (err) throw err;
  };

  /*
  
        Constructor.
  
        @param {leveldb.Handle} [handle] Pass a database handle to use with
          `batch.write()` or `batch.writeSync()`.
  */

  function Batch(handle) {
    this.handle = handle;
    this.self = new binding.Batch;
  }

  /*
  
        Add a put operation to the batch.
  
        @param {String|Buffer} key The key to put.
        @param {String|Buffer} val The value to put.
  */

  Batch.prototype.put = function(key, val) {
    if (!Buffer.isBuffer(key)) key = new Buffer(key);
    if (!Buffer.isBuffer(val)) val = new Buffer(val);
    this.self.put(key, val);
    return this;
  };

  /*
  
        Add a delete operation to the batch.
  
        @param {String|Buffer} key The key to delete.
  */

  Batch.prototype.del = function(key) {
    if (!Buffer.isBuffer(key)) key = new Buffer(key);
    this.self.del(key);
    return this;
  };

  /*
  
        Commit the batch operations to disk synchronously.
        See `Handle.write()`.
  */

  Batch.prototype.write = function(options, callback) {
    var _this = this;
    if (!this.handle) throw new Error('No handle');
    if (typeof options === 'function') {
      callback = options;
      options = null;
    }
    callback || (callback = noop);
    return this.handle.write(this.self, options, function(err) {
      if (!err) _this.self.clear();
      return callback(err);
    });
  };

  /*
  
        Commit the batch operations to disk synchronously. See `Batch.write()`.
  */

  Batch.prototype.writeSync = function(options) {
    if (!this.handle) throw new Error('No handle');
    this.handle.write(this.self, options);
    this.self.clear();
    return this;
  };

  /*
  
        Reset this batch instance by removing all pending operations.
  */

  Batch.prototype.clear = function() {
    return this.self.clear();
  };

  return Batch;

})();
