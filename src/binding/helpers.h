#ifndef NODE_LEVELDB_HELPERS_H_
#define NODE_LEVELDB_HELPERS_H_

#include <vector>

#include <v8.h>
#include <node.h>
#include <node_buffer.h>

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "node_async_shim.h"

using namespace node;
using namespace v8;

namespace node_leveldb {

static inline Handle<Value> ThrowTypeError(const char* err) {
  return ThrowException(Exception::TypeError(String::New(err)));
}

static inline Handle<Value> ThrowError(const char* err) {
  return ThrowException(Exception::Error(String::New(err)));
}

static inline leveldb::Slice ToSlice(
  Handle<Value> value, std::vector< Persistent<Value> >& buffers)
{
  if (Buffer::HasInstance(value)) {
    buffers.push_back(Persistent<Value>::New(value));
    Local<Object> obj = value->ToObject();
    return leveldb::Slice(Buffer::Data(obj), Buffer::Length(obj));
  } else {
    return leveldb::Slice();
  }
}

static inline leveldb::Slice ToSlice(Handle<Value> value, Persistent<Value>& buf) {
  if (Buffer::HasInstance(value)) {
    buf = Persistent<Value>::New(value);
    Local<Object> obj = value->ToObject();
    return leveldb::Slice(Buffer::Data(obj), Buffer::Length(obj));
  } else {
    return leveldb::Slice();
  }
}

static inline Handle<Value> ToBuffer(const leveldb::Slice& val) {
  return Buffer::New(String::New(val.data(), val.size()));
}

static inline Handle<Value> ToString(const leveldb::Slice& val) {
  return String::New(val.data(), val.size());
}

static inline Local<Function> GetCallback(const Arguments& args) {
  int idx = args.Length() - 1;
  if (args[idx]->IsFunction()) return Local<Function>::Cast(args[idx]);
  return Local<Function>();
}

} // node_leveldb

#endif // NODE_LEVELDB_HELPERS_H_
