#ifndef NODE_LEVELDB_SNAPSHOT_H_
#define NODE_LEVELDB_SNAPSHOT_H_

#include <leveldb/db.h>
#include <node.h>
#include <v8.h>

#include "DB.h"

using namespace node;
using namespace v8;

namespace node_leveldb {

class DB;
class Options;

class Snapshot : ObjectWrap {
 public:
  virtual ~Snapshot();

  void Close();
  bool Valid();

  const leveldb::Snapshot* operator*();

  static Persistent<FunctionTemplate> constructor;
  static bool HasInstance(Handle<Value> val);

  static void Init(Handle<Object> target);

  static Handle<Value> New(const Arguments& args);
  static Handle<Value> Valid(const Arguments& args);
  static Handle<Value> Close(const Arguments& args);

 private:
  const leveldb::Snapshot* snapshot;
  Persistent<Object> db_;

  inline leveldb::DB* db() { return **ObjectWrap::Unwrap<DB>(db_); }

  // No instance creation outside of Snapshot
  Snapshot(const leveldb::Snapshot* snapshot) : snapshot(snapshot) {};

  // No copying allowed
  Snapshot(const Snapshot&);
  void operator=(const Snapshot&);
};

} // namespace node_leveldb

#endif  // NODE_LEVELDB_SNAPSHOT_H_
