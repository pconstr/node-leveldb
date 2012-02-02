#ifndef NODE_LEVELDB_HANDLE_H_
#define NODE_LEVELDB_HANDLE_H_

#include <assert.h>

#include <string>
#include <vector>

#include <leveldb/db.h>
#include <node.h>
#include <v8.h>

#include "batch.h"
#include "node_async_shim.h"

using namespace node;
using namespace v8;

namespace node_leveldb {

class JHandle : public ObjectWrap {
 public:
  static Persistent<FunctionTemplate> constructor;
  static void Initialize(Handle<Object> target);

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> Open(const Arguments& args);
  static Handle<Value> Close(const Arguments& args);
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
  leveldb::DB* db_;
  const leveldb::Snapshot* snapshot_;
  std::vector< Persistent<Object> > iterators_;
  std::vector< Persistent<Object> > snapshots_;

  JHandle(leveldb::DB* db);
  virtual ~JHandle();

  void Close();
  bool Valid();

  JHandle(leveldb::DB* db, const leveldb::Snapshot* snapshot);

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
    ReadParams(JHandle *self, leveldb::Slice& slice, leveldb::ReadOptions &options, bool asBuffer, Handle<Function> cb)
      : Params(self, cb), slice(slice), options(options), asBuffer(asBuffer) {}

    virtual ~ReadParams() {
      if (!slice.empty()) delete slice.data();
    }

    leveldb::Slice slice;
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

  static async_rtn Destroy(uv_work_t* req);
  static async_rtn Repair(uv_work_t* req);

  static async_rtn After(uv_work_t* req);
  static async_rtn Close(uv_work_t* req);
  static async_rtn Write(uv_work_t* req);
};

} // namespace node_leveldb

#endif // NODE_LEVELDB_HANDLE_H_
