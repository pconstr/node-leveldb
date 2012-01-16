#include <vector>

#include <leveldb/write_batch.h>
#include <node.h>
#include <node_buffer.h>
#include <v8.h>

#include "DB.h"
#include "helpers.h"
#include "WriteBatch.h"

namespace node_leveldb {

Persistent<FunctionTemplate> WriteBatch::persistent_function_template;

WriteBatch::WriteBatch() {
}

WriteBatch::~WriteBatch() {
}

void WriteBatch::Init(Handle<Object> target) {
  HandleScope scope;

  Local<FunctionTemplate> local_function_template = FunctionTemplate::New(New);
  persistent_function_template = Persistent<FunctionTemplate>::New(local_function_template);
  persistent_function_template->InstanceTemplate()->SetInternalFieldCount(1);
  persistent_function_template->SetClassName(String::NewSymbol("WriteBatch"));

  NODE_SET_PROTOTYPE_METHOD(persistent_function_template, "put", Put);
  NODE_SET_PROTOTYPE_METHOD(persistent_function_template, "del", Del);
  NODE_SET_PROTOTYPE_METHOD(persistent_function_template, "clear", Clear);

  target->Set(String::NewSymbol("WriteBatch"), persistent_function_template->GetFunction());
}


//
// Constructor
//

Handle<Value> WriteBatch::New(const Arguments& args) {
  HandleScope scope;

  WriteBatch* writeBatch = new WriteBatch();
  writeBatch->Wrap(args.This());

  return args.This();
}


//
// Put
//

#define USAGE_ERROR(msg) \
  return ThrowTypeError(msg ": WriteBatch.put(<key>, <value>)")

Handle<Value> WriteBatch::Put(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 2)
    USAGE_ERROR("Invalid number of arguments");

  if (!args[0]->IsString() && !Buffer::HasInstance(args[0]))
    USAGE_ERROR("Argument 1 must be a string or buffer");

  if (!args[1]->IsString() && !Buffer::HasInstance(args[1]))
    USAGE_ERROR("Argument 2 must be a string or buffer");

  WriteBatch* self = ObjectWrap::Unwrap<WriteBatch>(args.This());

  leveldb::Slice key = JsToSlice(args[0], &self->strings);
  leveldb::Slice value = JsToSlice(args[1], &self->strings);

  self->wb.Put(key, value);

  return args.This();
}

#undef USAGE_ERROR


//
// Del
//

#define USAGE_ERROR(msg) \
  return ThrowTypeError(msg ": WriteBatch.del(<key>)")

Handle<Value> WriteBatch::Del(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 1)
    USAGE_ERROR("Invalid number of arguments");

  if (!args[0]->IsString() && !Buffer::HasInstance(args[0]))
    USAGE_ERROR("Argument 1 must be a string or buffer");

  WriteBatch* self = ObjectWrap::Unwrap<WriteBatch>(args.This());
  leveldb::Slice key = JsToSlice(args[0], &self->strings);

  self->wb.Delete(key);

  return args.This();
}

#undef USAGE_ERROR


//
// Clear
//

Handle<Value> WriteBatch::Clear(const Arguments& args) {
  HandleScope scope;

  WriteBatch* self = ObjectWrap::Unwrap<WriteBatch>(args.This());
  self->wb.Clear();
  self->strings.clear();

  return args.This();
}

}  // namespace node_leveldb
