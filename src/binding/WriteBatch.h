#ifndef WRITE_BATCH_H_
#define WRITE_BATCH_H_

#include <vector>

#include <leveldb/write_batch.h>
#include <node.h>
#include <v8.h>

#include "DB.h"

using namespace v8;
using namespace node;

namespace node_leveldb {

class WriteBatch : ObjectWrap {
 public:
  WriteBatch();
  virtual ~WriteBatch();

  static void Init(Handle<Object> target);

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> Put(const Arguments& args);
  static Handle<Value> Del(const Arguments& args);
  static Handle<Value> Clear(const Arguments& args);

 private:
  friend class DB;

  leveldb::WriteBatch wb;
  std::vector<std::string> strings;

  static Persistent<FunctionTemplate> persistent_function_template;
};

}  // node_leveldb

#endif  // WRITE_BATCH_H_
