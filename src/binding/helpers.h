#ifndef NODE_LEVELDB_HELPERS_H_
#define NODE_LEVELDB_HELPERS_H_

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <vector>

#include <v8.h>
#include <node.h>
#include <node_version.h>
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

static inline bool IsStringOrBuffer(Handle<Value> val) {
  return val->IsString() || Buffer::HasInstance(val);
}

static inline leveldb::Slice ToSlice(Handle<Value> value, std::vector<char*>* buffers = NULL) {
  if (value->IsString()) {
    Local<String> str = value->ToString();
    int len = str->Utf8Length();
    if (str.IsEmpty() || len <= 0) return leveldb::Slice();
    char* buf = new char[len + 1];
    str->WriteUtf8(buf);
    if (buffers) buffers->push_back(buf);
    return leveldb::Slice(buf, len);
  } else if (Buffer::HasInstance(value)) {
    Local<Object> obj = value->ToObject();
    int len = Buffer::Length(obj);
    if (len <= 0) return leveldb::Slice();
    char* buf = new char[len];
    memcpy(buf, Buffer::Data(obj), len);
    if (buffers) buffers->push_back(buf);
    return leveldb::Slice(buf, len);
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
