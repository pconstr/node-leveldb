#ifndef NODE_LEVELDB_OPTIONS_H_
#define NODE_LEVELDB_OPTIONS_H_

#include <leveldb/options.h>
#include <node.h>
#include <v8.h>

using namespace node;
using namespace v8;

namespace node_leveldb {

void ToOptions(Handle<Value> val, leveldb::Options &options);
void ToReadOptions(Handle<Value> val, leveldb::ReadOptions &options, bool &asBuffer);
void ToWriteOptions(Handle<Value> val, leveldb::WriteOptions &options);

} // namespace node_leveldb

#endif  // NODE_LEVELDB_OPTIONS_H_
