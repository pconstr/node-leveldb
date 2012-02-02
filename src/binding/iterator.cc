#include <assert.h>

#include <iostream>

#include <leveldb/iterator.h>
#include <node.h>
#include <v8.h>

#include "handle.h"
#include "helpers.h"
#include "iterator.h"

namespace node_leveldb {

Persistent<FunctionTemplate> JIterator::constructor;

JIterator::JIterator(Handle<Object>& db, leveldb::Iterator* it) : it_(it) {
  db_ = Persistent<Object>::New(db);
}

JIterator::~JIterator() {
  Close();
  db_.Dispose();
}

void JIterator::Close() {
  if (it_) {
    delete it_;
    it_ = NULL;
  }
};

bool JIterator::Valid() {
  return !db_.IsEmpty() && it_ && it_->Valid();
}

void JIterator::Initialize(Handle<Object> target) {
  HandleScope scope;

  Local<FunctionTemplate> t = FunctionTemplate::New(New);
  constructor = Persistent<FunctionTemplate>::New(t);
  constructor->InstanceTemplate()->SetInternalFieldCount(1);
  constructor->SetClassName(String::NewSymbol("Iterator"));

  NODE_SET_PROTOTYPE_METHOD(constructor, "valid", Valid);
  NODE_SET_PROTOTYPE_METHOD(constructor, "first", First);
  NODE_SET_PROTOTYPE_METHOD(constructor, "last", Last);
  NODE_SET_PROTOTYPE_METHOD(constructor, "seek", Seek);
  NODE_SET_PROTOTYPE_METHOD(constructor, "next", Next);
  NODE_SET_PROTOTYPE_METHOD(constructor, "prev", Prev);
  NODE_SET_PROTOTYPE_METHOD(constructor, "key", key);
  NODE_SET_PROTOTYPE_METHOD(constructor, "value", value);
  NODE_SET_PROTOTYPE_METHOD(constructor, "status", status);
}

Handle<Value> JIterator::New(const Arguments& args) {
  HandleScope scope;

  assert(args.Length() >= 2);
  assert(args[0]->IsObject() && JHandle::HasInstance(args[0]));
  assert(args[1]->IsExternal());

  Handle<Object> db = args[0]->ToObject();
  leveldb::Iterator* it = (leveldb::Iterator*) Local<External>::Cast(args[1])->Value();

  assert(it);

  JIterator* iterator = new JIterator(db, it);
  iterator->Wrap(args.This());

  return args.This();
}

Handle<Value> JIterator::Valid(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());
  return self->Valid() ? True() : False();
}

Handle<Value> JIterator::Seek(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (!self->Valid())
    return ThrowTypeError("Handle closed");

  if (args.Length() < 1 || !(args[0]->IsString() || Buffer::HasInstance(args[0])))
    return ThrowTypeError("Invalid arguments");

  leveldb::Slice key = ToSlice(args[0]);

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    self->it_->Seek(key);
  } else {
    SeekParams *data = new SeekParams(self, key, callback);
    BEGIN_ASYNC(data, Seek, After);
  }

  return Undefined();
}

Handle<Value> JIterator::First(const Arguments& args) {
  HandleScope scope;

  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (!self->Valid())
    return ThrowTypeError("Handle closed");

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    self->it_->SeekToFirst();
  } else {
    Params *data = new Params(self, callback);
    BEGIN_ASYNC(data, First, After);
  }

  return Undefined();
}

Handle<Value> JIterator::Last(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (!self->Valid())
    return ThrowTypeError("Handle closed");

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    self->it_->SeekToLast();
  } else {
    Params *data = new Params(self, callback);
    BEGIN_ASYNC(data, Last, After);
  }

  return Undefined();
}

Handle<Value> JIterator::Next(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (!self->Valid())
    return ThrowTypeError("Handle closed");

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    self->it_->Next();
  } else {
    Params *data = new Params(self, callback);
    BEGIN_ASYNC(data, Prev, After);
  }

  return Undefined();
}

Handle<Value> JIterator::Prev(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (!self->Valid())
    return ThrowTypeError("Handle closed");

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    self->it_->Prev();
  } else {
    Params *data = new Params(self, callback);
    BEGIN_ASYNC(data, Prev, After);
  }

  return Undefined();
}

static inline bool AsBuffer(const Arguments& args) {
  if (args.Length() > 1 && args[0]->IsObject()) {
    Local<Object> obj = args[0]->ToObject();

    static const Persistent<String> kAsBuffer = NODE_PSYMBOL("as_buffer");

    if (obj->Has(kAsBuffer))
      return obj->Get(kAsBuffer)->ToBoolean()->BooleanValue();
  }

  return false;
}

Handle<Value> JIterator::status(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (!self->Valid())
    return ThrowTypeError("Handle closed");

  leveldb::Status status = self->it_->status();

  static const Persistent<String> kOK = NODE_PSYMBOL("OK");

  if (status.ok()) return kOK;

  Handle<String> message = String::New(status.ToString().c_str());
  return scope.Close(Exception::Error(message));
}

Handle<Value> JIterator::key(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (!self->Valid())
    return ThrowTypeError("Handle closed");

  bool asBuffer = AsBuffer(args);

  leveldb::Slice val = self->it_->key();

  if (asBuffer) {
    return scope.Close(ToBuffer(val));
  } else {
    return scope.Close(ToString(val));
  }
}

Handle<Value> JIterator::value(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (!self->Valid())
    return ThrowTypeError("Handle closed");

  bool asBuffer = AsBuffer(args);

  leveldb::Slice val = self->it_->value();

  if (asBuffer) {
    return scope.Close(ToBuffer(val));
  } else {
    return scope.Close(ToString(val));
  }
}

} // node_leveldb
