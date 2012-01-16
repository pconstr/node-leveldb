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
  #define EIO_RETURN_TYPE void
  #define EIO_RETURN_STMT return
#else
  #define EIO_RETURN_TYPE int
  #define EIO_RETURN_STMT return 0
#endif

#define GET_CALLBACK_ARG(args, argv) (                      \
  ((argv) > 1 && (args)[(argv) - 1]->IsFunction())          \
    ? Local<Function>::Cast((args)[(argv) - 1])             \
    : Local<Function>()                                     \
  )

#define GET_WRITE_OPTIONS_ARG(options, args, argv, idx)     \
  if ((argv) >= (idx) && (args)[(idx)]->IsObject() && !(args)[(idx)]->IsFunction()) { \
    ToWriteOptions((args)[(idx)], options);                 \
  }

#define GET_READ_OPTIONS_ARG(options, asBool, args, argv, idx) \
  if ((argv) >= (idx) && (args)[(idx)]->IsObject() && !(args)[(idx)]->IsFunction()) { \
    ToReadOptions((args)[(idx)], (options), (asBool));      \
  }

#define GET_OPTIONS_ARG(options, args, argv, idx)           \
  if ((argv) >= (idx) && (args)[(idx)]->IsObject() && !(args)[(idx)]->IsFunction()) { \
    ToOptions((args)[(idx)], (options));                    \
  }

namespace node_leveldb {

  inline Handle<Value> ThrowTypeError(const char* err) {
    return ThrowException(Exception::TypeError(String::New(err)));
  }

  inline Handle<Value> ThrowError(const char* err) {
    return ThrowException(Exception::Error(String::New(err)));
  }

  // Helper to convert vanilla JS objects into leveldb objects
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
