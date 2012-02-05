#include <vector>

#include <leveldb/write_batch.h>
#include <node.h>
#include <node_buffer.h>
#include <v8.h>

#include "batch.h"
#include "helpers.h"

namespace node_leveldb {

Persistent<FunctionTemplate> JBatch::constructor;

void JBatch::Clear() {
  std::vector<char*>::iterator it;
  for (it = buffers_.begin(); it < buffers_.end(); ++it) free(*it);
  buffers_.clear();
  wb_.Clear();
}

void JBatch::Initialize(Handle<Object> target) {
  HandleScope scope;

  Local<FunctionTemplate> t = FunctionTemplate::New(New);
  constructor = Persistent<FunctionTemplate>::New(t);
  constructor->InstanceTemplate()->SetInternalFieldCount(1);
  constructor->SetClassName(String::NewSymbol("Batch"));

  NODE_SET_PROTOTYPE_METHOD(constructor, "put", Put);
  NODE_SET_PROTOTYPE_METHOD(constructor, "del", Del);
  NODE_SET_PROTOTYPE_METHOD(constructor, "clear", Clear);

  target->Set(String::NewSymbol("Batch"), constructor->GetFunction());
}

Handle<Value> JBatch::New(const Arguments& args) {
  HandleScope scope;

  JBatch* writeBatch = new JBatch();
  writeBatch->Wrap(args.This());

  return args.This();
}

Handle<Value> JBatch::Put(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 2 ||
      !IsStringOrBuffer(args[0]) || !IsStringOrBuffer(args[1]))
    return ThrowTypeError("Invalid arguments");

  JBatch* self = ObjectWrap::Unwrap<JBatch>(args.This());

  leveldb::Slice key = ToSlice(args[0], &self->buffers_);
  leveldb::Slice value = ToSlice(args[1], &self->buffers_);

  self->wb_.Put(key, value);

  return args.This();
}

Handle<Value> JBatch::Del(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 1 || !IsStringOrBuffer(args[0]))
    return ThrowTypeError("Invalid arguments");

  JBatch* self = ObjectWrap::Unwrap<JBatch>(args.This());
  leveldb::Slice key = ToSlice(args[0], &self->buffers_);

  self->wb_.Delete(key);

  return args.This();
}

Handle<Value> JBatch::Clear(const Arguments& args) {
  HandleScope scope;

  JBatch* self = ObjectWrap::Unwrap<JBatch>(args.This());
  self->Clear();

  return args.This();
}

} // namespace node_leveldb
