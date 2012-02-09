#ifndef NODE_LEVELDB_ITERATOR_H_
#define NODE_LEVELDB_ITERATOR_H_

#include <leveldb/iterator.h>
#include <node.h>
#include <pthread.h>
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

  inline void Lock() {
    pthread_mutex_lock(&lock_);
  }

  inline void Unlock() {
    pthread_mutex_unlock(&lock_);
  }

  inline bool Valid() {
    Lock();
    bool ok = it_ != NULL && it_->Valid();
    Unlock();
    return ok;
  }

  inline bool Seek(leveldb::Slice& key, leveldb::Status& status) {
    Lock();
    bool ok = it_ != NULL;
    if (ok) {
      it_->Seek(key);
      status = it_->status();
    }
    Unlock();
    return !ok;
  }

  inline bool First(leveldb::Status& status) {
    Lock();
    bool ok = it_ != NULL;
    if (ok) {
      it_->SeekToFirst();
      status = it_->status();
    }
    Unlock();
    return !ok;
  }

  inline bool Last(leveldb::Status& status) {
    Lock();
    bool ok = it_ != NULL;
    if (ok) {
      it_->SeekToLast();
      status = it_->status();
    }
    Unlock();
    return !ok;
  }

  inline bool Next(leveldb::Status& status) {
    Lock();
    bool ok = it_ != NULL && it_->Valid();
    if (ok) {
      it_->Next();
      status = it_->status();
    }
    Unlock();
    return !ok;
  }

  inline bool Prev(leveldb::Status& status) {
    Lock();
    bool ok = it_ != NULL && it_->Valid();
    if (ok) {
      it_->Prev();
      status = it_->status();
    }
    Unlock();
    return !ok;
  }

  inline bool key(leveldb::Slice& key) {
    Lock();
    bool ok = it_ != NULL && it_->Valid();
    if (ok) key = it_->key();
    Unlock();
    return !ok;
  }

  inline bool value(leveldb::Slice& val) {
    Lock();
    bool ok = it_ != NULL && it_->Valid();
    if (ok) val = it_->value();
    Unlock();
    return !ok;
  }

  inline bool current(leveldb::Slice& key, leveldb::Slice& val) {
    Lock();
    bool ok = it_ != NULL && it_->Valid();
    if (ok) {
      key = it_->key();
      val = it_->value();
    }
    Unlock();
    return !ok;
  }

  struct Params {
    Params(JIterator* self, Handle<Function>& cb) : self(self) {
      self->Ref();
      callback = Persistent<Function>::New(cb);
    }

    virtual ~Params() {
      self->Unref();
      callback.Dispose();
    }

    JIterator* self;
    Persistent<Function> callback;
    leveldb::Status status;
    bool error;
  };

  struct SeekParams : Params {
    SeekParams(JIterator* self,
               leveldb::Slice& key,
               Persistent<Value> keyHandle,
               Handle<Function>& cb)
      : Params(self, cb), key(key), keyHandle(keyHandle) {}

    virtual ~SeekParams() {
      if (!keyHandle.IsEmpty()) keyHandle.Dispose();
    }

    leveldb::Slice key;
    Persistent<Value> keyHandle;
  };

  static async_rtn After(uv_work_t* req);
  static async_rtn Seek(uv_work_t* req);
  static async_rtn First(uv_work_t* req);
  static async_rtn Last(uv_work_t* req);
  static async_rtn Next(uv_work_t* req);
  static async_rtn Prev(uv_work_t* req);

  leveldb::Iterator* it_;
  pthread_mutex_t lock_;
};

} // node_leveldb

#endif // NODE_LEVELDB_ITERATOR_H_
