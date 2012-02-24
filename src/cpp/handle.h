#ifndef NODE_LEVELDB_HANDLE_H_
#define NODE_LEVELDB_HANDLE_H_

#include <assert.h>

#include <vector>
#include <string>

#include <leveldb/db.h>
#include <node.h>
#include <v8.h>

#include "batch.h"
#include "iterator.h"
#include "node_async_shim.h"

using namespace node;
using namespace v8;

namespace node_leveldb {

class JHandle : public ObjectWrap {
 public:
  static Persistent<FunctionTemplate> constructor;
  static void Initialize(Handle<Object> target);

  static inline bool HasInstance(Handle<Value> value) {
    return value->IsObject() && constructor->HasInstance(value->ToObject());
  }

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> Open(const Arguments& args);
  static Handle<Value> Destroy(const Arguments& args);
  static Handle<Value> Repair(const Arguments& args);

  static Handle<Value> Read(const Arguments& args);
  static Handle<Value> Write(const Arguments& args);

  static Handle<Value> Iterator(const Arguments& args);
  static Handle<Value> Snapshot(const Arguments& args);

  static Handle<Value> Property(const Arguments& args);
  static Handle<Value> ApproximateSizes(const Arguments& args);
  static Handle<Value> CompactRange(const Arguments& args);

 private:
  JHandle(leveldb::DB* db) : ObjectWrap(), db_(db) {}

  ~JHandle() {
    if (db_ != NULL) {
      delete db_;
      db_ = NULL;
    }
    comparator_.Dispose();
  };

  inline bool Valid() {
    return db_ != NULL;
  };

  class async_params;
  class open_params;
  class dbop_params;
  class read_params;
  class write_params;
  class iterator_params;
  class snapshot_params;
  class property_params;
  class approxsizes_params;

  static Handle<Value> DbOp(const Arguments& args, const uv_work_cb async);

  static void AfterAsync(uv_work_t* req);
  static void AsyncOpen(uv_work_t* req);
  static void AsyncDestroy(uv_work_t* req);
  static void AsyncRepair(uv_work_t* req);
  static void AsyncRead(uv_work_t* req);
  static void AsyncWrite(uv_work_t* req);
  static void AsyncIterator(uv_work_t* req);
  static void AsyncSnapshot(uv_work_t* req);
  static void AsyncProperty(uv_work_t* req);
  static void AsyncApproxSizes(uv_work_t* req);

  Handle<Value> RefIterator(leveldb::Iterator* it);
  Handle<Value> RefSnapshot(const leveldb::Snapshot* snapshot);

  static void UnrefIterator(Persistent<Value> object, void* parameter);
  static void UnrefSnapshot(Persistent<Value> object, void* parameter);

  leveldb::DB* db_;
  Persistent<Value> comparator_;
};

} // namespace node_leveldb

#endif // NODE_LEVELDB_HANDLE_H_
