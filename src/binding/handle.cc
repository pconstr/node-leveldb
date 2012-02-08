#include <assert.h>
#include <limits.h>
#include <stdlib.h>

#include <algorithm>
#include <sstream>
#include <vector>

#include <leveldb/db.h>
#include <node.h>
#include <node_buffer.h>
#include <v8.h>

#include "batch.h"
#include "handle.h"
#include "helpers.h"
#include "iterator.h"
#include "options.h"

using namespace node;
using namespace v8;

namespace node_leveldb {

Persistent<FunctionTemplate> JHandle::constructor;

static inline Handle<Value> Error(const leveldb::Status& status) {
  if (status.IsNotFound()) {
    return Undefined();
  } else {
    return ThrowError(status.ToString().c_str());
  }
}

void JHandle::Initialize(Handle<Object> target) {
  HandleScope scope;

  Local<FunctionTemplate> t = FunctionTemplate::New(New);
  constructor = Persistent<FunctionTemplate>::New(t);
  constructor->InstanceTemplate()->SetInternalFieldCount(1);
  constructor->SetClassName(String::NewSymbol("Handle"));

  // Instance methods
  NODE_SET_PROTOTYPE_METHOD(constructor, "get", Get);
  NODE_SET_PROTOTYPE_METHOD(constructor, "write", Write);
  NODE_SET_PROTOTYPE_METHOD(constructor, "iterator", Iterator);
  NODE_SET_PROTOTYPE_METHOD(constructor, "snapshot", Snapshot);
  NODE_SET_PROTOTYPE_METHOD(constructor, "property", Property);
  NODE_SET_PROTOTYPE_METHOD(constructor, "approximateSizes", ApproximateSizes);
  NODE_SET_PROTOTYPE_METHOD(constructor, "compactRange", CompactRange);

  // Static methods
  NODE_SET_METHOD(target, "open", Open);
  NODE_SET_METHOD(target, "destroy", Destroy);
  NODE_SET_METHOD(target, "repair", Repair);

  // Set version
  std::stringstream version;
  version << leveldb::kMajorVersion << "." << leveldb::kMinorVersion;
  target->Set(String::New("bindingVersion"),
              String::New(version.str().c_str()),
              static_cast<PropertyAttribute>(ReadOnly|DontDelete));
}

Handle<Value> JHandle::New(const Arguments& args) {
  HandleScope scope;

  assert(args.Length() == 1);
  assert(args[0]->IsExternal());

  leveldb::DB* db = (leveldb::DB*)External::Unwrap(args[0]);
  JHandle* self = new JHandle(db);
  self->Wrap(args.This());

  return args.This();
}

Handle<Value> JHandle::Open(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 1 || !args[0]->IsString())
    return ThrowTypeError("Invalid arguments");

  // Required filename
  std::string name(*String::Utf8Value(args[0]));

  // Optional options
  leveldb::Options options;
  if (args.Length() > 1 && args[1]->IsObject() && !args[1]->IsFunction())
    UnpackOptions(args[1], options);

  // Optional callback
  Local<Function> callback = GetCallback(args);

  // Pass parameters to async function
  if (callback.IsEmpty()) {
    leveldb::DB* db;
    leveldb::Status status = leveldb::DB::Open(options, name, &db);
    if (!status.ok()) return Error(status);
    Local<Value> argc[] = { External::New(db) };
    Local<Object> self = constructor->GetFunction()->NewInstance(1, argc);
    return scope.Close(self);
  } else {
    OpenParams *params = new OpenParams(options, name, callback);
    BEGIN_ASYNC(params, Open, OpenAfter);
    return Undefined();
  }
}

Handle<Value> JHandle::Get(const Arguments& args) {
  HandleScope scope;

  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());
  int argc = args.Length();

  if (self->db_ == NULL) return ThrowError("Handle closed");

  if (argc < 1 || !Buffer::HasInstance(args[0]))
    return ThrowTypeError("Invalid arguments");

  bool asBuffer = false;

  // Key
  Persistent<Value> keyHandle;
  leveldb::Slice key = ToSlice(args[0], keyHandle);

  // Optional read options
  leveldb::ReadOptions options;
  if (argc > 1) UnpackReadOptions(args[1], options, asBuffer);

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    std::string result;
    if (!keyHandle.IsEmpty()) keyHandle.Dispose();
    leveldb::Status status = self->db_->Get(options, key, &result);
    if (!status.ok()) return Error(status);
    if (asBuffer) {
      return scope.Close(ToBuffer(result));
    } else {
      return scope.Close(ToString(result));
    }
  } else {
    ReadParams* params = new ReadParams(self, key, keyHandle, options, asBuffer, callback);
    BEGIN_ASYNC(params, Get, GetAfter);
  }

  return args.This();
}

Handle<Value> JHandle::Write(const Arguments& args) {
  HandleScope scope;

  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());
  int argc = args.Length();

  if (self->db_ == NULL) return ThrowError("Handle closed");

  if (argc < 1 || !JBatch::HasInstance(args[0]))
    return ThrowTypeError("Invalid arguments");

  JBatch* batch = ObjectWrap::Unwrap<JBatch>(args[0]->ToObject());

  // Optional write options
  leveldb::WriteOptions options;
  if (argc > 1) UnpackWriteOptions(args[1], options);

  // Optional callback
  Local<Function> callback = GetCallback(args);

  // Pass parameters to async function
  if (callback.IsEmpty()) {
    leveldb::Status status = self->db_->Write(options, &batch->wb_);
    if (!status.ok()) return ThrowError(status.ToString().c_str());
  } else {
    WriteParams *params = new WriteParams(self, options, batch, callback);
    BEGIN_ASYNC(params, Write, After);
  }

  return args.This();
}

void JHandle::UnrefIterator(Persistent<Value> object, void* parameter) {
  assert(object->IsObject());

  JHandle* self = (JHandle*)parameter;
  assert(self);

  self->Unref();
  object.Dispose();
}

Handle<Value> JHandle::Iterator(const Arguments& args) {
  HandleScope scope;
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());

  if (self->db_ == NULL) return ThrowError("Handle closed");

  bool asBuffer = false;

  leveldb::ReadOptions options;
  if (args.Length() > 0) UnpackReadOptions(args[0], options, asBuffer);

  leveldb::Iterator* it = self->db_->NewIterator(options);

  Local<Value> argv[] = { External::New(it) };
  Local<Object> instance = JIterator::constructor->GetFunction()->NewInstance(1, argv);

  // Keep a weak reference
  Persistent<Object> weak = Persistent<Object>::New(instance);
  weak.MakeWeak(self, &UnrefIterator);

  self->Ref();

  return scope.Close(instance);
}

void JHandle::UnrefSnapshot(Persistent<Value> object, void* parameter) {
  assert(object->IsExternal());

  JHandle* self = (JHandle*)parameter;
  leveldb::Snapshot* snapshot = (leveldb::Snapshot*)External::Unwrap(object);

  assert(handle);
  assert(snapshot);

  self->db_->ReleaseSnapshot(snapshot);
  self->Unref();

  object.Dispose();
}

Handle<Value> JHandle::Snapshot(const Arguments& args) {
  HandleScope scope;
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());

  if (self->db_ == NULL) return ThrowError("Handle closed");

  const leveldb::Snapshot* snapshot = self->db_->GetSnapshot();

  Local<Value> instance = External::New((void*)snapshot);
  Persistent<Value> weak = Persistent<Value>::New(instance);
  weak.MakeWeak(self, &UnrefSnapshot);

  self->Ref();

  return scope.Close(instance);
}

Handle<Value> JHandle::Property(const Arguments& args) {
  HandleScope scope;
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());

  if (self->db_ == NULL) return ThrowError("Handle closed");

  if (args.Length() < 1 || !args[0]->IsString())
    return ThrowTypeError("Invalid arguments");

  std::string name(*String::Utf8Value(args[0]));
  std::string value;

  if (self->db_->GetProperty(leveldb::Slice(name), &value)) {
    return scope.Close(String::New(value.c_str()));
  } else {
    return Undefined();
  }
}

Handle<Value> JHandle::ApproximateSizes(const Arguments& args) {
  HandleScope scope;
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());

  if (self->db_ == NULL) return ThrowError("Handle closed");

  if (args.Length() < 1 || !args[0]->IsArray())
    return ThrowTypeError("Invalid arguments");

  std::vector<leveldb::Range> ranges;
  std::vector< Persistent<Value> > handles;

  Local<Array> array(Array::Cast(*args[0]));
  int len = array->Length();

  if (len % 2 != 0)
    return ThrowTypeError("Invalid arguments");

  // Optional callback
  Local<Function> callback = GetCallback(args);

  for (int i = 0; i < len; i += 2) {
    if (array->Has(i) && array->Has(i + 1)) {
      Local<Value> lStart = array->Get(i);
      Local<Value> lLimit = array->Get(i + 1);

      leveldb::Slice start = ToSlice(lStart);
      leveldb::Slice limit = ToSlice(lLimit);

      if (!callback.IsEmpty()) {
        handles.push_back(Persistent<Value>::New(lStart));
        handles.push_back(Persistent<Value>::New(lLimit));
      }

      ranges.push_back(leveldb::Range(start, limit));
    }
  }

  int nRanges = ranges.size();
  uint64_t* sizes = new uint64_t[ nRanges ];
  self->db_->GetApproximateSizes(&ranges[0], nRanges, sizes);

  Local<Array> result = Array::New(nRanges);
  for (int i = 0; i < nRanges; ++i) {
    uint64_t size = sizes[i];
    if (size < INT_MAX) {
      result->Set(i, Integer::New(static_cast<uint32_t>(sizes[i])));
    } else {
      result->Set(i, Number::New(static_cast<double>(sizes[i])));
    }
  }

  delete[] sizes;

  if (!callback.IsEmpty()) {
    std::vector< Persistent<Value> >::iterator it;
    for (it = handles.begin(); it < handles.end(); ++it) it->Dispose();

    Handle<Value> argv[] = { Null(), result };
    callback->Call(args.This(), 2, argv);

    return args.This();
  } else {
    return scope.Close(result);
  }
}

Handle<Value> JHandle::CompactRange(const Arguments& args) {
  HandleScope scope;
  return ThrowError("Method not implemented");
}

Handle<Value> JHandle::Destroy(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 1 || !args[0]->IsString())
    return ThrowTypeError("Invalid arguments");

  // Required name
  String::Utf8Value name(args[0]);

  // Optional options
  leveldb::Options options;
  if (args.Length() > 1) UnpackOptions(args[1], options);

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    leveldb::Status status = leveldb::DestroyDB(*name, options);
    if (!status.ok()) return ThrowError(status.ToString().c_str());
  } else {
    OpenParams *params = new OpenParams(options, *name, callback);
    BEGIN_ASYNC(params, Destroy, After);
  }

  return Undefined();
}

Handle<Value> JHandle::Repair(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 1 || !args[0]->IsString())
    return ThrowTypeError("Invalid arguments");

  // Required name
  String::Utf8Value name(args[0]);

  // Optional options
  leveldb::Options options;
  if (args.Length() > 1) UnpackOptions(args[1], options);

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    leveldb::Status status = leveldb::RepairDB(*name, options);
    if (!status.ok()) return ThrowError(status.ToString().c_str());
  } else {
    OpenParams *params = new OpenParams(options, *name, callback);
    BEGIN_ASYNC(params, Repair, After);
  }

  return Undefined();
}

void JHandle::Params::Callback() {
  TryCatch try_catch;

  Handle<Object> that;
  if (self) {
    that = self->handle_;
  } else {
    that = Object::New();
  }

  if (status.IsNotFound()) {
    // not found, return (null, undef)
    Handle<Value> argv[] = { Null() };
    callback->Call(that, 1, argv);
  } else if (!status.ok()) {
    // error, callback with first argument as error object
    Local<String> message = String::New(status.ToString().c_str());
    Local<Value> argv[] = { Exception::Error(message) };
    callback->Call(that, 1, argv);
  } else {
    callback->Call(that, 0, NULL);
  }

  if (try_catch.HasCaught()) FatalException(try_catch);
}

void JHandle::Params::Callback(const Handle<Value>& result) {
  assert(!callback.IsEmpty());

  TryCatch try_catch;

  if (result.IsEmpty()) {
    callback->Call(self->handle_, 0, NULL);
  } else {
    Handle<Value> argv[] = { Null(), result };
    callback->Call(self->handle_, 2, argv);
  }

  if (try_catch.HasCaught()) FatalException(try_catch);
}


//
// ASYNC FUNCTIONS
//

async_rtn JHandle::Open(uv_work_t* req) {
  OpenParams* params = (OpenParams*) req->data;
  params->status = leveldb::DB::Open(params->options, params->name, &params->db);
  RETURN_ASYNC;
}

async_rtn JHandle::OpenAfter(uv_work_t* req) {
  HandleScope scope;
  OpenParams* params = (OpenParams*) req->data;

  if (params->status.ok()) {
    Local<Value> argc[] = { External::New(params->db) };
    Local<Object> self = constructor->GetFunction()->NewInstance(1, argc);
    JHandle* handle = ObjectWrap::Unwrap<JHandle>(self);
    handle->Ref();
    params->self = handle;
    params->Callback(self);
  } else {
    params->Callback();
  }

  delete params;
  RETURN_ASYNC_AFTER;
}

async_rtn JHandle::Get(uv_work_t* req) {
  ReadParams* params = (ReadParams*) req->data;
  params->status = params->self->db_->Get(params->options, params->key, &params->result);
  RETURN_ASYNC;
}

async_rtn JHandle::GetAfter(uv_work_t* req) {
  HandleScope scope;

  ReadParams* params = (ReadParams*) req->data;
  if (params->status.ok() && !params->status.IsNotFound()) {
    if (params->asBuffer) {
      params->Callback(ToBuffer(params->result));
    } else {
      params->Callback(ToString(params->result));
    }
  } else {
    params->Callback();
  }

  delete params;
  RETURN_ASYNC_AFTER;
}

async_rtn JHandle::After(uv_work_t* req) {
  Params *params = (Params*) req->data;
  params->Callback();
  delete params;
  RETURN_ASYNC_AFTER;
}

async_rtn JHandle::Write(uv_work_t* req) {
  WriteParams *params = (WriteParams*) req->data;
  params->status = params->self->db_->Write(params->options, &params->batch->wb_);
  RETURN_ASYNC;
}

async_rtn JHandle::Destroy(uv_work_t* req) {
  OpenParams* params = (OpenParams*) req->data;
  params->status = leveldb::DestroyDB(params->name, params->options)
  RETURN_ASYNC;
}

async_rtn JHandle::Repair(uv_work_t* req) {
  OpenParams* params = (OpenParams*) req->data;
  params->status = leveldb::RepairDB(params->name, params->options)
  RETURN_ASYNC;
}

} // namespace node_leveldb
