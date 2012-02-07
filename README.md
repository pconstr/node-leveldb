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

var db = leveldb.openSync("path/to/my/db", { create_if_missing: true });

var key = "mykey";
db.putSync(key, "My Value!");

var value = db.getSync(key);
console.dir(value); // prints: My Value!

db.delSync(key);

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


## Contributing

Since I am not experienced in the world of C++ and am still learning, I
welcome contributions.  Mainly what I need right now is someone with
experience to review my code and tell me things I'm doing stupid.  Maybe add
a C++ best pratices note to this document.  Also once the initial blocking
version of the library is done, I'll need to add async ability to all the
functions that do disk I/O.

Currently Randall Leeds (@tilgovi and one of the CouchDB commiters) has
expressed interest in helping and already has commit rights to the project.
