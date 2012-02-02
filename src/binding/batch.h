#ifndef NODE_LEVELDB_WRITE_BATCH_H_
#define NODE_LEVELDB_WRITE_BATCH_H_

#include <vector>

#include <leveldb/write_batch.h>
#include <node.h>
#include <v8.h>

using namespace v8;
using namespace node;

namespace node_leveldb {

class JBatch : ObjectWrap {
 public:
  JBatch() {}
  virtual ~JBatch() { Clear(); }

  static bool HasInstance(Handle<Value> val);

  static Persistent<FunctionTemplate> constructor;
  static void Initialize(Handle<Object> target);

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> Put(const Arguments& args);
  static Handle<Value> Del(const Arguments& args);
  static Handle<Value> Clear(const Arguments& args);

 private:
  friend class JHandle;

  void Clear();

  leveldb::WriteBatch wb_;
  std::vector<char*> buffers_;
};

} // node_leveldb

#endif // NODE_LEVELDB_WRITE_BATCH_H_
