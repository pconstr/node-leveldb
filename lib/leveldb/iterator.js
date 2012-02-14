var Iterator, binding;

binding = require('../leveldb.node');

/*

    An iterator allows sequential and random access to the database.

    Usage:

        var leveldb = require('leveldb');

        var db = leveldb.open('/tmp/test.db')
          , it = db.iterator();

        // iterator is initially invalid
        it.firstSync();

        // get a key
        it.get('foobar');
*/

exports.Iterator = Iterator = (function() {
  var noop;

  noop = function(err) {
    if (err) throw err;
  };

  /*
  
        Constructor.
  
        @param {Native} self The underlying iterator object.
  */

  function Iterator(self) {
    this.self = self;
  }

  /*
  
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
  */

  Iterator.prototype.forRange = function() {
    var args, callback, limit, limitKey, next, options, startKey,
      _this = this;
    args = Array.prototype.slice.call(arguments);
    callback = args.pop();
    if (!callback) throw new Error('Missing callback');
    if (typeof args[args.length - 1] === 'object') options = args.pop();
    if (args.length === 2) {
      startKey = args[0], limitKey = args[1];
    } else {
      startKey = args[0];
    }
    if (limitKey) limit = limitKey.toString('binary');
    next = function(err) {
      if (err) return callback(err);
      return _this.current(options, function(err, key, val) {
        if (err) return callback(err);
        if (key) {
          callback(null, key, val);
          if (!limit || limit !== key.toString('binary')) return _this.next(next);
        }
      });
    };
    if (startKey) {
      return this.seek(startKey, next);
    } else {
      return this.first(next);
    }
  };

  /*
  
        True if the iterator is positioned at a valid key.
  */

  Iterator.prototype.valid = function() {
    return this.self.valid();
  };

  /*
  
        Position the iterator at a key.
  
        @param {String} key The key at which to position the iterator.
        @param {Function} [callback] Optional callback.
          @param {Error} error The error value on error, null otherwise.
  */

  Iterator.prototype.seek = function(key, callback) {
    if (callback == null) callback = noop;
    if (!Buffer.isBuffer(key)) key = new Buffer(key);
    return this.self.seek(key, callback);
  };

  /*
  
        Synchronous version of `Iterator.seek()`.
  */

  Iterator.prototype.seekSync = function(key) {
    if (!Buffer.isBuffer(key)) key = new Buffer(key);
    return this.self.seek(key);
  };

  /*
  
        Position the iterator at the first key.
  
        @param {Function} [callback] Optional callback.
          @param {Error} error The error value on error, null otherwise.
  */

  Iterator.prototype.first = function(callback) {
    if (callback == null) callback = noop;
    return this.self.first(callback);
  };

  /*
  
        Synchronous version of `Iterator.first()`.
  */

  Iterator.prototype.firstSync = function() {
    return this.self.first();
  };

  /*
  
        Position the iterator at the last key.
  
        @param {Function} [callback] Optional callback.
          @param {Error} error The error value on error, null otherwise.
  */

  Iterator.prototype.last = function(callback) {
    if (callback == null) callback = noop;
    return this.self.last(callback);
  };

  /*
  
        Synchronous version of `Iterator.last()`.
  */

  Iterator.prototype.lastSync = function() {
    return this.self.last();
  };

  /*
  
        Advance the iterator to the next key.
  
        @param {Function} [callback] Optional callback.
          @param {Error} error The error value on error, null otherwise.
  */

  Iterator.prototype.next = function(callback) {
    if (callback == null) callback = noop;
    return this.self.next(callback);
  };

  /*
  
        Synchronous version of `Iterator.next()`.
  */

  Iterator.prototype.nextSync = function() {
    return this.self.next();
  };

  /*
  
        Advance the iterator to the previous key.
  
        @param {Function} [callback] Optional callback.
          @param {Error} error The error value on error, null otherwise.
  */

  Iterator.prototype.prev = function(callback) {
    if (callback == null) callback = noop;
    return this.self.prev(callback);
  };

  /*
  
        Synchronous version of `Iterator.prev()`.
  */

  Iterator.prototype.prevSync = function() {
    return this.self.prev();
  };

  /*
  
        Get the key at the current iterator position.
  
        @param {Object} [options] Optional options.
          @param {Boolean} [options.as_buffer=false] If true, data will be
            returned as a `Buffer`.
        @param {Function} [callback] Optional callback. If not given, returns
          the key synchronously.
          @param {Error} error The error value on error, null otherwise.
          @param {String|Buffer} key The key if successful.
  */

  Iterator.prototype.key = function(options, callback) {
    var key;
    if (options == null) options = {};
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }
    if (callback) {
      return this.self.key(function(err, key) {
        if (!(err || options.as_buffer)) key = key.toString('utf8');
        return callback(err, key);
      });
    } else {
      key = this.self.key();
      if (key && !options.as_buffer) key = key.toString('utf8');
      return key;
    }
  };

  /*
  
        Get the value at the current iterator position.
  
        @param {Object} [options] Optional options.
          @param {Boolean} [options.as_buffer=false] If true, data will be
            returned as a `Buffer`.
        @param {Function} [callback] Optional callback. If not given, returns
          the value synchronously.
          @param {Error} error The error value on error, null otherwise.
          @param {String|Buffer} value The value if successful.
  */

  Iterator.prototype.value = function(options, callback) {
    var val;
    if (options == null) options = {};
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }
    if (callback) {
      return this.self.value(function(err, val) {
        if (!(err || options.as_buffer)) val = val.toString('utf8');
        return callback(err, val);
      });
    } else {
      val = this.self.value();
      if (val && !options.as_buffer) val = val.toString('utf8');
      return val;
    }
  };

  /*
  
        Get the key and value at the current iterator position.
  
        @param {Object} [options] Optional options.
          @param {Boolean} [options.as_buffer=false] If true, data will be
            returned as a `Buffer`.
        @param {Function} [callback] Optional callback. If not given, returns
          the key synchronously.
          @param {Error} error The error value on error, null otherwise.
          @param {String|Buffer} key The key if successful.
          @param {String|Buffer} value The value if successful.
  */

  Iterator.prototype.current = function(options, callback) {
    var key, kv, val;
    if (options == null) options = {};
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }
    if (callback) {
      return this.self.current(function(err, kv) {
        var key, val;
        if (kv) {
          key = kv[0], val = kv[1];
          if (!(err || options.as_buffer)) {
            key = key.toString('utf8');
            val = val.toString('utf8');
          }
        }
        return callback(err, key, val);
      });
    } else {
      if (kv = this.self.current()) {
        key = kv[0], val = kv[1];
        if (!options.as_buffer) {
          val = val.toString('utf8');
          key = key.toString('utf8');
        }
      }
      return [key, val];
    }
  };

  return Iterator;

})();
