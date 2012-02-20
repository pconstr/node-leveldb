[![Build Status](https://secure.travis-ci.org/mikepb/node-leveldb.png)](http://travis-ci.org/mikepb/node-leveldb)

# Node-LevelDB

The leveldb library provides a persistent key value store. Keys and values
are arbitrary byte arrays. The keys are ordered within the key value store
according to a user-specified comparator function.

This project is node bindings to this excellent library so that node
programs can either implement their own custom databases or simply use it
directly as a fast, simple key/value store.

While implementing nStore, I realized there are a couple things that V8 and
node are not very good at.  They are heavy disk I/O and massive objects with
either millions of keys or millions of sub objects.

Since LevelDB provides good primitives like MVCC and binary support (It was
designed to back IndexDB in the Chrome browser), then it can be used as a
base to implement things like CouchDB.


## Usage

```js
var leveldb = require('leveldb');

leveldb.open("path/to/my/db", { create_if_missing: true }, onOpen);

function onOpen(err, db) {
  var key = "mykey";
  db.put(key, "My Value!", function(err) {
    db.getSync(key, function(err, value) {
      console.dir(value); // prints: My Value!
      db.del(key);
    });
  });
}

// db closes automatically when out of scope
```

## Install

```bash
npm install leveldb
```


## Compiling

So far, compiles on Ubuntu and Mac OS X Lion.

```bash
node-waf configure build
```

The leveldb library is bundled in the deps folder and is compiled in
staticly.  The resulting node addon is about 4mb unstripped.  If you want to
make it smaller, I've had success using the `strip` command on it which
shrinks it down to about 300kb.

```bash
strip build/leveldb.node
```

## Running tests

Tests are written using the [mocha](http://visionmedia.github.com/mocha/)
test framework. The test command automatically builds the required files
prior to running the tests.

```bash
npm test
```


## Contributors

```
   161  Michael Phan-Ba
    39  Tim Caswell
    22  Nathan Landis
    19  gabor@google.com
    15  jorlow@chromium.org
    12  dgrogan@chromium.org
    12  Stefan Thomas
     9  Carter Thaxton
     9  Randall Leeds
     6  Damon Oehlman
     5  Hans Wennborg
     4  shinuza
     3  hans@chromium.org
     2  Sanjay Ghemawat
     1  (no author)
     1  Gabor Cselle
```
