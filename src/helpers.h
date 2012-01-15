#ifndef HELPERS_H_
#define HELPERS_H_

#include <v8.h>
#include <node.h>
#include <node_version.h>
#include <node_buffer.h>

#include "leveldb/db.h"
#include <vector>
#include <string>
#include <iostream>   // for debugging

using namespace node;
using namespace v8;

#if NODE_VERSION_AT_LEAST(0, 5, 4)
  #define eio_return_type void
  #define eio_return_stmt return
#else
  #define eio_return_type int
  #define eio_return_stmt return 0
#endif

#define GET_CALLBACK_ARG(args, argv) (                      \
  ((argv) > 1 && (args)[(argv) - 1]->IsFunction())          \
    ? Local<Function>::Cast((args)[(argv) - 1])             \
    : Local<Function>()                                     \
  )

#define GET_WRITE_OPTIONS_ARG(args, argv, idx) (            \
  ((argv) >= (idx) && (args)[(idx)]->IsObject() && !(args)[(idx)]->IsFunction()) \
    ? JsToWriteOptions((args)[(idx)])                       \
    : leveldb::WriteOptions()                               \
  )

#define GET_READ_OPTIONS_ARG(asBool, args, argv, idx) (     \
  ((argv) >= (idx) && (args)[(idx)]->IsObject() && !(args)[(idx)]->IsFunction()) \
    ? JsToReadOptions((args)[(idx)], (asBool))              \
    : leveldb::ReadOptions()                                \
  )

#define GET_OPTIONS_ARG(args, argv, idx) (                  \
  ((argv) >= (idx) && (args)[(idx)]->IsObject() && !(args)[(idx)]->IsFunction()) \
    ? JsToOptions((args)[(idx)])                  \
    : leveldb::Options()                                    \
  )

namespace node_leveldb {

  inline Handle<Value> ThrowTypeError(const char* err) {
    return ThrowException(Exception::TypeError(String::New(err)));
  }

  inline Handle<Value> ThrowError(const char* err) {
    return ThrowException(Exception::Error(String::New(err)));
  }

  // Helper to convert vanilla JS objects into leveldb objects
  leveldb::Options JsToOptions(Handle<Value> val);
  leveldb::ReadOptions JsToReadOptions(Handle<Value> val, bool &asBuffer);
  leveldb::WriteOptions JsToWriteOptions(Handle<Value> val);
  leveldb::Slice JsToSlice(Handle<Value> val, std::vector<std::string> *strings);

  // Helper to convert a leveldb::Status instance to a V8 return value
  Handle<Value> processStatus(leveldb::Status status);

  // Helpers to work with buffers
  Local<Object> Bufferize(std::string value);
  char* BufferData(Buffer *b);
  size_t BufferLength(Buffer *b);
  char* BufferData(Handle<Object> buf_obj);
  size_t BufferLength(Handle<Object> buf_obj);

} // node_leveldb

#endif  // HELPERS_H_
