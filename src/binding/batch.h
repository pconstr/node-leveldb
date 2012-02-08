#ifndef NODE_LEVELDB_BATCH_H_
#define NODE_LEVELDB_BATCH_H_

#include <assert.h>
#include <pthread.h>

#include <vector>

#include <leveldb/write_batch.h>
#include <node.h>
#include <v8.h>

using namespace v8;
using namespace node;

namespace node_leveldb {

class JBatch : ObjectWrap {
 public:
  static inline bool HasInstance(Handle<Value> value) {
    return value->IsObject() && constructor->HasInstance(value->ToObject());
  }

  static Persistent<FunctionTemplate> constructor;
  static void Initialize(Handle<Object> target);

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> Put(const Arguments& args);
  static Handle<Value> Del(const Arguments& args);
  static Handle<Value> Clear(const Arguments& args);

 private:
  friend class JHandle;

  inline JBatch() : ObjectWrap() {
    assert(pthread_mutex_init(&lock_, NULL) == 0);
  }

  virtual ~JBatch() {
    Clear();
    assert(pthread_mutex_destroy(&lock_) == 0);
  }

  inline void Lock() {
    pthread_mutex_lock(&lock_);
  }

  inline void Unlock() {
    pthread_mutex_unlock(&lock_);
  }

  inline void Put(leveldb::Slice& key, leveldb::Slice& val) {
    Lock();
    wb_.Put(key, val);
    Unlock();
  }

  inline void Del(leveldb::Slice& key) {
    Lock();
    wb_.Delete(key);
    Unlock();
  }

  inline void Clear() {
    Lock();
    std::vector< Persistent<Value> >::iterator it;
    for (it = buffers_.begin(); it < buffers_.end(); ++it) it->Dispose();
    buffers_.clear();
    wb_.Clear();
    Unlock();
  }

  leveldb::WriteBatch wb_;
  std::vector< Persistent<Value> > buffers_;
  pthread_mutex_t lock_;
};

} // node_leveldb

#endif // NODE_LEVELDB_BATCH_H_
