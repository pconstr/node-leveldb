#ifndef NODE_LEVELDB_HANDLE_H_
#define NODE_LEVELDB_HANDLE_H_

#include <assert.h>

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
  };

  inline bool Valid() {
    return db_ != NULL;
  };

  inline Handle<Value> RefIterator(leveldb::Iterator* it) {
    Local<Value> argv[] = { External::New(it) };
    Local<Object> instance = JIterator::constructor->GetFunction()->NewInstance(1, argv);

    // Keep a weak reference
    Persistent<Object> weak = Persistent<Object>::New(instance);
    weak.MakeWeak(this, &UnrefIterator);

    Ref();

    return instance;
  }

  inline Handle<Value> RefSnapshot(const leveldb::Snapshot* snapshot) {
    Local<Value> instance = External::New((void*)snapshot);
    Persistent<Value> weak = Persistent<Value>::New(instance);
    weak.MakeWeak(this, &UnrefSnapshot);
    Ref();
    return instance;
  }

  static void UnrefIterator(Persistent<Value> object, void* parameter);
  static void UnrefSnapshot(Persistent<Value> object, void* parameter);

  class OpenOp;
  class OpenOp : public Operation<OpenOp> {
   public:

    inline OpenOp(const ExecFunction exec, const ConvFunction conv,
                  Handle<Object>& handle, Handle<Function>& callback)
      : Operation<OpenOp>(exec, conv, handle, callback), db_(NULL) {}

    std::string name_;

    leveldb::Options options_;
    leveldb::Status status_;
    leveldb::DB* db_;
  };

  class ReadOp;
  class ReadOp : public Operation<ReadOp> {
   public:

    inline ReadOp(const ExecFunction exec, const ConvFunction conv,
                  Handle<Object>& handle, Handle<Function>& callback)
      : Operation<ReadOp>(exec, conv, handle, callback)
    {
      self_ = ObjectWrap::Unwrap<JHandle>(handle);
      self_->Ref();
    }

    virtual ~ReadOp() {
      self_->Unref();
      keyHandle_.Dispose();
    }

    inline Handle<Value> Before() {
      if (self_->db_ == NULL) return ThrowError("Handle closed");
      return Handle<Value>();
    }

    JHandle* self_;

    leveldb::Slice key_;
    leveldb::Status status_;
    leveldb::ReadOptions options_;

    std::string result_;

    Persistent<Value> keyHandle_;
  };

  class WriteOp;
  class WriteOp : public Operation<WriteOp> {
   public:

    inline WriteOp(const ExecFunction exec, const ConvFunction conv,
                   Handle<Object>& handle, Handle<Function>& callback)
      : Operation<WriteOp>(exec, conv, handle, callback)
    {
      self_ = ObjectWrap::Unwrap<JHandle>(handle);
      self_->Ref();
    }

    virtual ~WriteOp() {
      self_->Unref();
      batch_->Unref();
    }

    inline Handle<Value> Before() {
      if (self_->db_ == NULL) return ThrowError("Handle closed");
      return Handle<Value>();
    }

    JHandle* self_;
    JBatch* batch_;

    leveldb::Status status_;
    leveldb::WriteOptions options_;
  };

  class IteratorOp;
  class IteratorOp : public Operation<IteratorOp> {
   public:

    inline IteratorOp(const ExecFunction exec, const ConvFunction conv,
                      Handle<Object>& handle, Handle<Function>& callback)
      : Operation<IteratorOp>(exec, conv, handle, callback)
    {
      self_ = ObjectWrap::Unwrap<JHandle>(handle);
      self_->Ref();
    }

    virtual ~IteratorOp() {
      self_->Unref();
    }

    inline Handle<Value> Before() {
      if (self_->db_ == NULL) return ThrowError("Handle closed");
      return Handle<Value>();
    }

    JHandle* self_;

    leveldb::Iterator* it_;
    leveldb::Status status_;
    leveldb::ReadOptions options_;
  };

  class SnapshotOp;
  class SnapshotOp : public Operation<SnapshotOp> {
   public:

    inline SnapshotOp(const ExecFunction exec, const ConvFunction conv,
                      Handle<Object>& handle, Handle<Function>& callback)
      : Operation<SnapshotOp>(exec, conv, handle, callback)
    {
      self_ = ObjectWrap::Unwrap<JHandle>(handle);
      self_->Ref();
    }

    virtual ~SnapshotOp() {
      self_->Unref();
    }

    inline Handle<Value> Before() {
      if (self_->db_ == NULL) return ThrowError("Handle closed");
      return Handle<Value>();
    }

    JHandle* self_;

    leveldb::Status status_;
    leveldb::Snapshot* snap_;
  };

  static void Open(OpenOp* op);
  static void Open(OpenOp* op, Handle<Value>& error, Handle<Value>& result);

  static void Destroy(OpenOp* op);
  static void Repair(OpenOp* op);

  static void OpenConv(OpenOp* op, Handle<Value>& error, Handle<Value>& result);

  static void Read(ReadOp* op);
  static void Read(ReadOp* op, Handle<Value>& error, Handle<Value>& result);

  static void Write(WriteOp* op);
  static void Write(WriteOp* op, Handle<Value>& error, Handle<Value>& result);

  static void Iterator(IteratorOp* op);
  static void Iterator(IteratorOp* op,
                       Handle<Value>& error, Handle<Value>& result);

  static void Snapshot(SnapshotOp* op);
  static void Snapshot(SnapshotOp* op,
                       Handle<Value>& error, Handle<Value>& result);

  leveldb::DB* db_;
};

} // namespace node_leveldb

#endif // NODE_LEVELDB_HANDLE_H_
