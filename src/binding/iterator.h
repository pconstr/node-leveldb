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
    assert(pthread_mutex_destroy(&lock_) == 0);
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
    assert(pthread_mutex_init(&lock_, NULL) == 0);
  }

  // No copying allowed
  JIterator(const JIterator&);
  void operator=(const JIterator&);

  inline bool Lock() {
    int err = pthread_mutex_trylock(&lock_);
    assert(err == 0 || err != EBUSY);
    return err == EBUSY;
  }

  inline void Unlock() {
    assert(pthread_mutex_unlock(&lock_) == 0);
  }

  inline bool Valid() {
    return it_ != NULL && it_->Valid();
  }

  inline bool key(leveldb::Slice& key) {
    if (it_ == NULL || !it_->Valid()) return true;
    key = it_->key();
    return false;
  }

  inline bool value(leveldb::Slice& val) {
    if (it_ == NULL || !it_->Valid()) return true;
    val = it_->value();
    return false;
  }

  inline bool current(leveldb::Slice& key, leveldb::Slice& val) {
    if (it_ == NULL || !it_->Valid()) return true;
    key = it_->key();
    val = it_->value();
    return false;
  }

  struct Op;

  typedef void (*RunFunction)(Op* operation);

  static void RunSeek(Op* data);
  static void RunFirst(Op* data);
  static void RunLast(Op* data);
  static void RunNext(Op* data);
  static void RunPrev(Op* data);

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

    inline void Result(Handle<Value>& error, Handle<Value>& result) {
      if (invalidState_) {
        error = Exception::Error(String::New("Illegal state"));
      } else if (!status_.ok()) {
        error = Exception::Error(String::New(status_.ToString().c_str()));
      }
    }

    void ReturnAsync();

    RunFunction run_;

    JIterator* it_;

    leveldb::Status status_;
    leveldb::Slice key_;

    Persistent<Function> callback_;
    Persistent<Value> keyHandle_;

    bool invalidState_;
  };

  leveldb::Iterator* it_;
  pthread_mutex_t lock_;
};

} // node_leveldb

#endif // NODE_LEVELDB_ITERATOR_H_
