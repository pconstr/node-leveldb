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
  : ObjectWrap(), db_(db), snapshot_(NULL) {}

JHandle::JHandle(leveldb::DB* db, const leveldb::Snapshot* snapshot)
  : ObjectWrap(), db_(db), snapshot_(snapshot) {}

JHandle::~JHandle() {
  Close();
}

void JHandle::Close() {
  if (db_ != NULL) {
    if (snapshot_ != NULL) {
      db_->ReleaseSnapshot(snapshot_);
    } else {
      std::vector< Persistent<Object> >::iterator it;
      for (it = iterators_.begin(); it != iterators_.end(); it++) {
        JIterator *iterator = ObjectWrap::Unwrap<JIterator>(*it);
        iterator->Close();
      }
      iterators_.clear();
      for (it = snapshots_.begin(); it != snapshots_.end(); it++) {
        JHandle *handle = ObjectWrap::Unwrap<JHandle>(*it);
        handle->Close();
      }
      snapshots_.clear();
      delete db_;
    }
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

  int argc = args.Length();
  assert(argc >= 1);
  assert(args[0]->IsExternal());

  leveldb::DB* db = (leveldb::DB*)External::Unwrap(args[0]);
  JHandle* self;

  if (argc > 1) {
    assert(args[1]->IsTrue());
    self = new JHandle(db, db->GetSnapshot());
  } else {
    self = new JHandle(db);
  }

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

static bool IsNotNearDeath(Persistent<Object> o) {
  return !o.IsNearDeath();
}

static void UnrefIterator(Persistent<Value> object, void* parameter) {
  std::vector< Persistent<Object> > *ilist =
    (std::vector< Persistent<Object> > *) parameter;

  std::vector< Persistent<Object> >::iterator it =
    partition(ilist->begin(), ilist->end(), IsNotNearDeath);

  ilist->erase(it, ilist->end());
}

static inline Handle<Value> NewDependentInstance(
  const Persistent<FunctionTemplate>& constructor,
  int argc, Handle<Value>* args,
  std::vector< Persistent<Object> >& objects)
{
  Handle<Object> obj = constructor->GetFunction()->NewInstance(argc, args);

  // Keep a weak reference
  Persistent<Object> weak = Persistent<Object>::New(obj);
  weak.MakeWeak(&objects, &UnrefIterator);
  objects.push_back(weak);

  return obj;
}

Handle<Value> JHandle::Iterator(const Arguments& args) {
  HandleScope scope;
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());

  if (self->db_ == NULL) return ThrowError("Handle closed");

  bool asBuffer = false;

  leveldb::ReadOptions options;
  if (args.Length() > 0) UnpackReadOptions(args[0], options, asBuffer);

  leveldb::Iterator* it = self->db_->NewIterator(options);

  Handle<Value> iteratorArgs[] = { args.This(), External::New(it) };
  Handle<Value> iterator =
    NewDependentInstance(JIterator::constructor, 2, iteratorArgs, self->iterators_);

  return scope.Close(iterator);
}

Handle<Value> JHandle::Snapshot(const Arguments& args) {
  HandleScope scope;
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());

  if (self->db_ == NULL) return ThrowError("Handle closed");

  Handle<Value> handleArgs[] = { External::New(self->db_), True() };
  Handle<Value> handle =
    NewDependentInstance(constructor, 2, handleArgs, self->snapshots_);

  return scope.Close(handle);
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

static inline void AddRange(
  std::vector<leveldb::Range>& ranges,
  Handle<Value> startValue,
  Handle<Value> limitValue,
  std::vector<char*>& buffers)
{
  leveldb::Slice start = ToSlice(startValue, &buffers);
  leveldb::Slice limit = ToSlice(limitValue, &buffers);

  if (start.empty() || limit.empty()) return;

  ranges.push_back(leveldb::Range(start, limit));
}

Handle<Value> JHandle::ApproximateSizes(const Arguments& args) {
  HandleScope scope;
  JHandle* self = ObjectWrap::Unwrap<JHandle>(args.This());

  if (self->db_ == NULL) return ThrowError("Handle closed");

  if (args.Length() < 1)
    return ThrowTypeError("Invalid arguments");

  std::vector<leveldb::Range> ranges;
  std::vector<char*> buffers;

  if (args[0]->IsArray()) {

    Local<Array> array(Array::Cast(*args[0]));
    int len = array->Length();

    for (int i = 0; i < len; ++i) {
      if (array->Has(i)) {
        Local<Value> elem = array->Get(i);
        if (elem->IsArray()) {
          Local<Array> bounds(Array::Cast(*elem));
          if (bounds->Length() >= 2) {
            AddRange(ranges, bounds->Get(0), bounds->Get(1), buffers);
          }
        }
      }
    }

  } else if (args.Length() >= 2 &&
    (args[0]->IsString() || Buffer::HasInstance(args[0])) &&
    (args[1]->IsString() || Buffer::HasInstance(args[1]))) {

    AddRange(ranges, args[0], args[1], buffers);

  } else {
    return ThrowTypeError("Invalid arguments");
  }

  uint64_t sizes = 0;

  self->db_->GetApproximateSizes(&ranges[0], ranges.size(), &sizes);

  std::vector<char*>::iterator it;
  for (it = buffers.begin(); it < buffers.end(); ++it) {
    delete *it;
  }

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
