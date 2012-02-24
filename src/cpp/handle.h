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

  class iterator_params;
  class snapshot_params;

  static void AsyncIterator(uv_work_t* req);
  static void AsyncSnapshot(uv_work_t* req);

  Handle<Value> RefIterator(leveldb::Iterator* it);
  Handle<Value> RefSnapshot(const leveldb::Snapshot* snapshot);

  static void UnrefIterator(Persistent<Value> object, void* parameter);
  static void UnrefSnapshot(Persistent<Value> object, void* parameter);

  template <class T>
  class Op : public Operation<T> {
   public:

    typedef void (*ExecFunction)(T* op);
    typedef void (*ConvFunction)(
      T* op, Handle<Value>& error, Handle<Value>& result);

    inline Op(const ExecFunction exec, const ConvFunction conv,
              Handle<Object>& handle, Handle<Function>& callback)
      : Operation<T>(exec, conv, handle, callback)
    {
      self_ = ObjectWrap::Unwrap<JHandle>(handle);
      self_->Ref();
    }

    virtual ~Op() {
      self_->Unref();
    }

    virtual inline Handle<Value> BeforeRun() {
      if (self_->db_ == NULL) return ThrowError("Handle closed");
      return Handle<Value>();
    }

    JHandle* self_;

    leveldb::Status status_;

  };

  class ApproximateSizesOp;
  class ApproximateSizesOp : public Op<ApproximateSizesOp> {
   public:

    inline ApproximateSizesOp(const ExecFunction exec, const ConvFunction conv,
                              Handle<Object>& handle, Handle<Function>& callback)
      : Op<ApproximateSizesOp>(exec, conv, handle, callback), sizes_(NULL) {}

    virtual ~ApproximateSizesOp() {
      std::vector< Persistent<Value> >::iterator it;
      for (it = handles_.begin(); it < handles_.end(); ++it) it->Dispose();
      if (sizes_) delete[] sizes_;
    }

    std::vector<leveldb::Range> ranges_;
    std::vector< Persistent<Value> > handles_;

    uint64_t* sizes_;
  };

  static void ApproximateSizes(ApproximateSizesOp* op);
  static void ApproximateSizes(ApproximateSizesOp* op,
                               Handle<Value>& error, Handle<Value>& result);

  leveldb::DB* db_;
  Persistent<Value> comparator_;
};

} // namespace node_leveldb

#endif // NODE_LEVELDB_HANDLE_H_
