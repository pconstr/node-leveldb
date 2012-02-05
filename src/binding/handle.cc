#include <assert.h>
#include <stdlib.h>

#include <algorithm>
#include <sstream>
#include <vector>

#include <leveldb/db.h>
#include <node.h>
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

JHandle::JHandle(leveldb::DB* db)
  : ObjectWrap(), db_(db) {}

JHandle::~JHandle() {
  Close();
}

void JHandle::Close() {
  if (db_ != NULL) {

    std::vector< const leveldb::Snapshot* >::iterator it;
    for (it = snapshots_.begin(); it < snapshots_.end(); ++it) {
      db_->ReleaseSnapshot(*it);
    }

    std::vector<JIterator*>::iterator jt;
    for (jt = iterators_.begin(); jt < iterators_.end(); ++jt) {
      (*jt)->Close();
    }

    snapshots_.clear();
    iterators_.clear();

    delete db_;
    db_ = NULL;
  }
};

bool JHandle::Valid() {
  return db_ != NULL;
};





void JHandle::Initialize(Handle<Object> target) {
  HandleScope scope; // used by v8 for garbage collection

  // Our constructor
  Local<FunctionTemplate> t = FunctionTemplate::New(New);
  constructor = Persistent<FunctionTemplate>::New(t);
  constructor->InstanceTemplate()->SetInternalFieldCount(1);
  constructor->SetClassName(String::NewSymbol("Handle"));

  // Instance methods
  NODE_SET_PROTOTYPE_METHOD(constructor, "close", Close);
  NODE_SET_PROTOTYPE_METHOD(constructor, "valid", Valid);
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
    Handle<Object> self = constructor->GetFunction()->NewInstance(1, argc);
    return scope.Close(self);
  } else {
    OpenParams *params = new OpenParams(options, name, callback);
    BEGIN_ASYNC(params, Open, OpenAfter);
    return Undefined();
  }
}

Handle<Value> JHandle::Close(const Arguments& args) {
  HandleScope scope;

  // Get this and arguments
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());

  if (self->db_ == NULL) return ThrowError("Handle closed");

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    self->Close();
  } else {
    Params* params = new Params(self, callback);
    BEGIN_ASYNC(params, Close, After);
  }

  return Undefined();
}

Handle<Value> JHandle::Valid(const Arguments& args) {
  HandleScope scope;
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());
  return self->db_ != NULL ? True() : False();
}

Handle<Value> JHandle::Get(const Arguments& args) {
  HandleScope scope;

  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());
  int argc = args.Length();

  if (self->db_ == NULL) return ThrowError("Handle closed");

  if (argc < 1 || !IsStringOrBuffer(args[0]))
    return ThrowTypeError("Invalid arguments");

  bool asBuffer = false;

  // Key
  leveldb::Slice slice = ToSlice(args[0]);

  // Optional read options
  leveldb::ReadOptions options;
  if (argc > 1) UnpackReadOptions(args[1], options, asBuffer);

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    std::string result;
    if (self->db_ == NULL) {
      leveldb::Status status = self->db_->Get(options, slice, &result);
      delete slice.data();
      if (!status.ok()) return Error(status);
      if (asBuffer) {
        return scope.Close(ToBuffer(result));
      } else {
        return scope.Close(ToString(result));
      }
    }
    delete slice.data();
  } else {
    ReadParams* params = new ReadParams(self, slice, options, asBuffer, callback);
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

  JIterator* it = ObjectWrap::Unwrap<JIterator>(object->ToObject());
  JHandle* handle = (JHandle*)parameter;

  assert(it);
  assert(handle);

  remove(handle->iterators_.begin(), handle->iterators_.end(), it);

  it->Close();
  object.Dispose();
  handle->Unref();
}

Handle<Value> JHandle::Iterator(const Arguments& args) {
  HandleScope scope;
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());

  if (self->db_ == NULL) return ThrowError("Handle closed");

  bool asBuffer = false;

  leveldb::ReadOptions options;
  if (args.Length() > 0) UnpackReadOptions(args[0], options, asBuffer);

  leveldb::Iterator* it = self->db_->NewIterator(options);

  Handle<Value> argv[] = { External::New(it) };
  Handle<Object> instance = JIterator::constructor->GetFunction()->NewInstance(1, argv);

  // Keep a weak reference
  Persistent<Object> weak = Persistent<Object>::New(instance);
  weak.MakeWeak(self, &UnrefIterator);
  JIterator* iterator = ObjectWrap::Unwrap<JIterator>(instance);
  self->iterators_.push_back(iterator);
  self->Ref();

  return scope.Close(instance);
}

void JHandle::UnrefSnapshot(Persistent<Value> object, void* parameter) {
  assert(object->IsExternal());

  JHandle* handle = (JHandle*)parameter;
  leveldb::Snapshot* snapshot = (leveldb::Snapshot*)External::Unwrap(object);

  assert(handle);
  assert(snapshot);

  handle->db_->ReleaseSnapshot(snapshot);
  remove(handle->snapshots_.begin(), handle->snapshots_.end(), snapshot);

  object.Dispose();
}

Handle<Value> JHandle::Snapshot(const Arguments& args) {
  HandleScope scope;
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());

  if (self->db_ == NULL) return ThrowError("Handle closed");

  const leveldb::Snapshot* snapshot = self->db_->GetSnapshot();
  self->snapshots_.push_back(snapshot);

  Handle<Value> instance = External::New((void*)snapshot);
  Persistent<Value> weak = Persistent<Value>::New(instance);
  weak.MakeWeak(self, &UnrefSnapshot);

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
  std::vector<char*> buffers;

  Local<Array> array(Array::Cast(*args[0]));
  int len = array->Length();

  for (int i = 0; i < len; ++i) {
    if (array->Has(i)) {
      Local<Value> elem = array->Get(i);

      if (elem->IsArray()) {
        Local<Array> bounds(Array::Cast(*elem));

        if (bounds->Length() >= 2) {
          leveldb::Slice start = ToSlice(bounds->Get(0), &buffers);
          leveldb::Slice limit = ToSlice(bounds->Get(1), &buffers);

          if (!start.empty() && !limit.empty())
            ranges.push_back(leveldb::Range(start, limit));

        }
      }
    }
  }

  uint64_t sizes = 0;
  self->db_->GetApproximateSizes(&ranges[0], ranges.size(), &sizes);

  std::vector<char*>::iterator it;
  // FIXME: memory leak, but breaks if free'd
  //for (it = buffers.begin(); it < buffers.end(); ++it) delete *it;

  return scope.Close(Integer::New((int32_t)sizes));
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

} // namespace node_leveldb
