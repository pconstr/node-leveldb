#ifndef NODE_LEVELDB_ITERATOR_H_
#define NODE_LEVELDB_ITERATOR_H_

#include <assert.h>
#include <errno.h>
#include <pthread.h>

#include <leveldb/iterator.h>
#include <node.h>
#include <v8.h>

#include "handle.h"
#include "helpers.h"

using namespace v8;
using namespace node;

namespace node_leveldb {

class JHandle;

class JIterator : ObjectWrap {
 public:
  virtual ~JIterator() {
    if (it_) {
      delete it_;
      it_ = NULL;
    }
    int err = pthread_mutex_destroy(&lock_);
    assert(err == 0);
  }

  static Persistent<FunctionTemplate> constructor;

  static void Initialize(Handle<Object> target);
  static Handle<Value> New(const Arguments& args);

  static Handle<Value> Valid(const Arguments& args);
  static Handle<Value> Seek(const Arguments& args);
  static Handle<Value> First(const Arguments& args);
  static Handle<Value> Last(const Arguments& args);
  static Handle<Value> Next(const Arguments& args);
  static Handle<Value> Prev(const Arguments& args);

  static Handle<Value> key(const Arguments& args);
  static Handle<Value> value(const Arguments& args);
  static Handle<Value> current(const Arguments& args);

 private:
  friend class JHandle;

  // No instance creation outside of Handle
  inline JIterator(leveldb::Iterator* it) : ObjectWrap(), it_(it) {
    int err = pthread_mutex_init(&lock_, NULL);
    assert(err == 0);
  }

  // No copying allowed
  JIterator(const JIterator&);
  void operator=(const JIterator&);

  inline bool Lock() {
    int err = pthread_mutex_trylock(&lock_);
    assert(err == 0 || err == EBUSY);
    return err == EBUSY;
  }

  inline void Unlock() {
    int err = pthread_mutex_unlock(&lock_);
    assert(err == 0);
  }

  struct Op;

  typedef void (*RunFunction)(Op* operation);

  static inline Handle<Value> RunOp(RunFunction run, const Arguments& args) {
    HandleScope scope;
    JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

    // Optional callback
    Local<Function> callback = GetCallback(args);

    // Build operation
    Op* op = new Op(run, self, callback);

    return op->Run();
  }

  static void RunSeek(Op* data);
  static void RunFirst(Op* data);
  static void RunLast(Op* data);
  static void RunNext(Op* data);
  static void RunPrev(Op* data);

  static void RunGetKey(Op* data);
  static void RunGetValue(Op* data);
  static void RunGetKeyValue(Op* data);

  static async_rtn AsyncOp(uv_work_t* req);
  static async_rtn AfterOp(uv_work_t* req);

  struct Op {

    inline Op(RunFunction run, JIterator* it, Handle<Function>& callback)
      : run_(run), it_(it), invalidState_(false)
    {
      callback_ = Persistent<Function>::New(callback);
      it_->Ref();
    }

    virtual ~Op() {
      callback_.Dispose();
      keyHandle_.Dispose();
      it_->Unref();
    }

    inline void Exec() {
      if (it_->it_ == NULL) {
        invalidState_ = true;
        return;
      }
      run_(this);
    }

    Handle<Value> RunSync();

    inline Handle<Value> RunAsync() {
      BEGIN_ASYNC(this, AsyncOp, AfterOp);
      return Undefined();
    }

    inline Handle<Value> Run() {
      if (it_->Lock())
        return ThrowError("Concurrent operations not supported");
      return callback_.IsEmpty() ? RunSync() : RunAsync();
    }

    void ReturnAsync();
    void Result(Handle<Value>& error, Handle<Value>& result);

    RunFunction run_;

    JIterator* it_;

    leveldb::Status status_;
    leveldb::Slice key_;
    leveldb::Slice value_;

    Persistent<Function> callback_;
    Persistent<Value> keyHandle_;

    bool invalidState_;
  };

  leveldb::Iterator* it_;
  pthread_mutex_t lock_;
};

} // node_leveldb

#endif // NODE_LEVELDB_ITERATOR_H_
