#ifndef NODE_LEVELDB_ITERATOR_H_
#define NODE_LEVELDB_ITERATOR_H_

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
  ~JIterator();

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
  static Handle<Value> status(const Arguments& args);

 private:
  friend class JHandle;

  leveldb::Iterator* it_;
  Persistent<Object> db_;

  // No instance creation outside of DB
  JIterator(Handle<Object>& db, leveldb::Iterator* it);

  // No copying allowed
  JIterator(const JIterator&);
  void operator=(const JIterator&);

  inline bool Valid() {
    return !db_.IsEmpty() && it_;
  }

  struct Params {
    Params(JIterator* self, Handle<Function>& cb) : self(self) {
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

    JIterator* self;
    Persistent<Function> callback;
  };

  struct SeekParams : Params {
    SeekParams(JIterator* self, leveldb::Slice& key, Handle<Function>& cb)
      : Params(self, cb), key(key) {}

    virtual ~SeekParams() {
      if (!key.empty()) delete key.data();
    }

    leveldb::Slice key;
  };

  static async_rtn After(uv_work_t* req);
  static async_rtn Seek(uv_work_t* req);
  static async_rtn First(uv_work_t* req);
  static async_rtn Last(uv_work_t* req);
  static async_rtn Next(uv_work_t* req);
  static async_rtn Prev(uv_work_t* req);

};

} // node_leveldb

#endif // NODE_LEVELDB_ITERATOR_H_
