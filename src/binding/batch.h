#ifndef NODE_LEVELDB_BATCH_H_
#define NODE_LEVELDB_BATCH_H_

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

  inline JBatch() : ObjectWrap() {}
  virtual ~JBatch() { Clear(); }

  inline void Clear() {
    std::vector< Persistent<Value> >::iterator it;
    for (it = buffers_.begin(); it < buffers_.end(); ++it) it->Dispose();
    buffers_.clear();
    wb_.Clear();
  }

  leveldb::WriteBatch wb_;
  std::vector< Persistent<Value> > buffers_;
};

} // node_leveldb

#endif // NODE_LEVELDB_BATCH_H_
