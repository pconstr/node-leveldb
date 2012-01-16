#ifndef OPTIONS_H_
#define OPTIONS_H_

#include <leveldb/options.h>
#include <node.h>
#include <v8.h>

using namespace v8;

namespace node_leveldb {

void ToOptions(Handle<Value> val, leveldb::Options &options);
void ToReadOptions(Handle<Value> val, leveldb::ReadOptions &options, bool &asBuffer);
void ToWriteOptions(Handle<Value> val, leveldb::WriteOptions &options);

} // namespace node_leveldb

#endif  // OPTIONS_H_
