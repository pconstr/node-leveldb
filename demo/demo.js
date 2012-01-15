var leveldb = require('../build/leveldb.node'),
    DB = leveldb.DB,
    Iterator = leveldb.Iterator,
    WriteBatch = leveldb.WriteBatch;

var path = __dirname + "/testdb";

var db = new DB();

// Opening
function open() {
  console.log("Opening...");
  var status = db.open(path, {create_if_missing: true, paranoid_checks: true}, put);
  console.log(status);
}

// Putting
var key = new Buffer("Hello");
var value = new Buffer("World");
function put() {
  console.log("\nPutting...");
  var status = db.put(key, value, get);
  console.log(status);
}

// Getting
function get() {
  console.log("\nGetting...");
  db.get(key, function(err, value) {
    if(value.toString() === value.toString()) {
      console.log("OK");
    } else {
      console.log("FAIL");
    }
    bulkWrite();
  });
}

// Bulk writing
function bulkWrite() {
  console.log("\nBulk writing...");
  var wb = new WriteBatch();
  wb.put(new Buffer('foo'), new Buffer('bar'));
  wb.put(new Buffer('booz'), new Buffer('baz'));
  var status = db.write(wb, bulkWriteCb);
  console.log("Bulk writing: putting: " + status);
}

function bulkWriteCb() {
  db.get(new Buffer('booz'), function(err, val1) {
    db.get(new Buffer('foo'), function(err, val2) {
      if (val1.toString() == 'baz' && val2.toString() == 'bar') {
        var status = "OK";
      } else {
        var status = "FAIL";
      }
      console.log("Bulk writing: getting: " + status);
      del();
    });
  });
}

// Deleting
function del() {
  console.log("\nDeleting...");
  var status = db.del(key, close);
  console.log(status);

}

// Closing
function close() {
  console.log("\nClosing...");
  db.close();
  console.log("OK");
}

open();
