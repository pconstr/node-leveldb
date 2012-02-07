#ifndef NODE_LEVELDB_HANDLE_H_
#define NODE_LEVELDB_HANDLE_H_

#include <assert.h>

#include <string>

#include <leveldb/db.h>
#include <node.h>
#include <v8.h>

#include "batch.h"
#include "node_async_shim.h"

using namespace node;
using namespace v8;

namespace node_leveldb {

class JIterator;

class JHandle : public ObjectWrap {
 public:
  static Persistent<FunctionTemplate> constructor;
  static void Initialize(Handle<Object> target);

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> Open(const Arguments& args);
  static Handle<Value> Valid(const Arguments& args);

  static Handle<Value> Get(const Arguments& args);
  static Handle<Value> Write(const Arguments& args);

  static Handle<Value> Iterator(const Arguments& args);
  static Handle<Value> Snapshot(const Arguments& args);

  static Handle<Value> Property(const Arguments& args);
  static Handle<Value> ApproximateSizes(const Arguments& args);
  static Handle<Value> CompactRange(const Arguments& args);

  static Handle<Value> Destroy(const Arguments& args);
  static Handle<Value> Repair(const Arguments& args);

 private:
  JHandle(leveldb::DB* db) : ObjectWrap(), db_(db) {}

  ~JHandle() {
    if (db_ != NULL) {
      delete db_;
      db_ = NULL;
    }
  };

  inline bool Valid() {
    return db_ != NULL;
  };

  static void UnrefIterator(Persistent<Value> object, void* parameter);
  static void UnrefSnapshot(Persistent<Value> object, void* parameter);

  struct Params {
    Params(JHandle* self, Handle<Function> cb) : self(self) {
      if (self) self->Ref();
      callback = Persistent<Function>::New(cb);
    }

    virtual ~Params() {
      if (self) {
        self->Unref();
        self = NULL;
      }
      callback.Dispose();
    }

    virtual void Callback();
    virtual void Callback(const Handle<Value>& result);

    JHandle* self;
    Persistent<Function> callback;
    leveldb::Status status;
  };

  struct OpenParams : Params {
    OpenParams(leveldb::Options &options, std::string name, Handle<Function> cb)
      : Params(NULL, cb), options(options), name(name) {}
    leveldb::DB* db;
    leveldb::Options options;
    std::string name;
  };

  struct ReadParams : Params {
    ReadParams(JHandle *self,
               leveldb::Slice& key,
               Persistent<Value> keyHandle,
               leveldb::ReadOptions &options,
               bool asBuffer,
               Handle<Function> cb)
      : Params(self, cb),
        key(key),
        keyHandle(keyHandle),
        options(options),
        asBuffer(asBuffer) {}

    virtual ~ReadParams() {
      if (!keyHandle.IsEmpty()) keyHandle.Dispose();
    }

    leveldb::Slice key;
    Persistent<Value> keyHandle;
    leveldb::ReadOptions options;
    bool asBuffer;
    std::string result;
  };

  struct WriteParams : Params {
    WriteParams(JHandle *self, leveldb::WriteOptions &options, JBatch *batch,
                Handle<Function> callback)
      : Params(self, callback), options(options), batch(batch)
    {
      batch->Ref();
    }

    virtual ~WriteParams() {
      batch->Unref();
    }

    leveldb::WriteOptions options;
    JBatch *batch;
  };

  static async_rtn Open(uv_work_t* req);
  static async_rtn OpenAfter(uv_work_t* req);

  static async_rtn Get(uv_work_t* req);
  static async_rtn GetAfter(uv_work_t* req);

  static async_rtn After(uv_work_t* req);

  static async_rtn Write(uv_work_t* req);

  static async_rtn Destroy(uv_work_t* req);
  static async_rtn Repair(uv_work_t* req);

  leveldb::DB* db_;
};

} // namespace node_leveldb

#endif // NODE_LEVELDB_HANDLE_H_
