#ifndef NODE_LEVELDB_ITERATOR_H_
#define NODE_LEVELDB_ITERATOR_H_

#include <assert.h>
#include <errno.h>

#include <leveldb/iterator.h>
#include <node.h>
#include <v8.h>

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
  }

  static Persistent<FunctionTemplate> constructor;

  static void Initialize(Handle<Object> target);
  static Handle<Value> New(const Arguments& args);

  static Handle<Value> Seek(const Arguments& args);
  static Handle<Value> First(const Arguments& args);
  static Handle<Value> Last(const Arguments& args);
  static Handle<Value> Next(const Arguments& args);
  static Handle<Value> Prev(const Arguments& args);

  static Handle<Value> GetKey(const Arguments& args);
  static Handle<Value> GetKeyValue(const Arguments& args);

 private:
  friend class JHandle;

  // No instance creation outside of Handle
  inline JIterator(leveldb::Iterator* it) : ObjectWrap(), it_(it) {}

  // No copying allowed
  JIterator(const JIterator&);
  void operator=(const JIterator&);

  class iter_params;
  class seek_params;
  class kv_params;

  static void AsyncSeek(uv_work_t* req);
  static void AsyncFirst(uv_work_t* req);
  static void AsyncLast(uv_work_t* req);
  static void AsyncNext(uv_work_t* req);
  static void AsyncPrev(uv_work_t* req);

  static void AsyncGetKey(uv_work_t* req);
  static void AsyncGetKeyValue(uv_work_t* req);

  static void AfterAsync(uv_work_t* req);

  leveldb::Iterator* it_;
};

} // node_leveldb

#endif // NODE_LEVELDB_ITERATOR_H_
