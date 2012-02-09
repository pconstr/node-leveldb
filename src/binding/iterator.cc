#include <assert.h>
#include <pthread.h>
#include <stdlib.h>

#include <leveldb/iterator.h>
#include <node.h>
#include <v8.h>

#include "handle.h"
#include "helpers.h"
#include "iterator.h"

namespace node_leveldb {

Persistent<FunctionTemplate> JIterator::constructor;

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
  NODE_SET_PROTOTYPE_METHOD(constructor, "current", current);
}

Handle<Value> JIterator::New(const Arguments& args) {
  HandleScope scope;

  assert(args.Length() == 1);
  assert(args[0]->IsExternal());

  leveldb::Iterator* it = (leveldb::Iterator*)External::Unwrap(args[0]);

  assert(it);

  JIterator* iterator = new JIterator(it);
  iterator->Wrap(args.This());

  return args.This();
}

Handle<Value> JIterator::Valid(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (self->Lock()) return ThrowError("Concurrent operations not supported");
  bool valid = self->Valid();
  self->Unlock();

  return valid ? True() : False();
}

Handle<Value> JIterator::Seek(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (args.Length() < 1 || !Buffer::HasInstance(args[0]))
    return ThrowTypeError("Invalid arguments");

  leveldb::Slice key = ToSlice(args[0]);

  // Optional callback
  Local<Function> callback = GetCallback(args);

  // Build operation
  Op* op = new Op(&RunSeek, self, callback);
  op->key_ = key;
  op->keyHandle_ = Persistent<Value>::New(args[0]);

  return op->Run();
}

Handle<Value> JIterator::First(const Arguments& args) {
  HandleScope scope;

  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  // Optional callback
  Local<Function> callback = GetCallback(args);

  // Build operation
  Op* op = new Op(&RunFirst, self, callback);

  return op->Run();
}

Handle<Value> JIterator::Last(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  // Optional callback
  Local<Function> callback = GetCallback(args);

  // Build operation
  Op* op = new Op(&RunLast, self, callback);

  return op->Run();
}

Handle<Value> JIterator::Next(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  // Optional callback
  Local<Function> callback = GetCallback(args);

  // Build operation
  Op* op = new Op(&RunNext, self, callback);

  return op->Run();
}

Handle<Value> JIterator::Prev(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  // Optional callback
  Local<Function> callback = GetCallback(args);

  // Build operation
  Op* op = new Op(&RunPrev, self, callback);

  return op->Run();
}

Handle<Value> JIterator::key(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  leveldb::Slice key;
  bool invalidState = false;

  if (self->Lock()) return ThrowError("Concurrent operations not supported");
  invalidState = self->key(key);
  self->Unlock();

  if (invalidState) return Undefined();

  return scope.Close(ToBuffer(key));
}

Handle<Value> JIterator::value(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  leveldb::Slice val;
  bool invalidState = false;

  if (self->Lock()) return ThrowError("Concurrent operations not supported");
  invalidState = self->value(val);
  self->Unlock();

  if (invalidState) return Undefined();

  return scope.Close(ToBuffer(val));
}

Handle<Value> JIterator::current(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  leveldb::Slice key;
  leveldb::Slice val;
  bool invalidState = false;

  if (self->Lock()) return ThrowError("Concurrent operations not supported");
  invalidState = self->current(key, val);
  self->Unlock();

  if (invalidState) return Undefined();

  Local<Array> pair = Array::New(2);

  pair->Set(0, ToBuffer(key));
  pair->Set(1, ToBuffer(val));

  return scope.Close(pair);
}


//
// ASYNC FUNCTIONS
//


void JIterator::RunSeek(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  if (it == NULL) {
    data->invalidState_ = true;
    return;
  }
  it->Seek(data->key_);
  data->status_ = it->status();
}

void JIterator::RunFirst(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  if (it == NULL) {
    data->invalidState_ = true;
    return;
  }
  it->SeekToFirst();
  data->status_ = it->status();
}

void JIterator::RunLast(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  if (it == NULL) {
    data->invalidState_ = true;
    return;
  }
  it->SeekToLast();
  data->status_ = it->status();
}

void JIterator::RunNext(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  if (it == NULL || !it->Valid()) {
    data->invalidState_ = true;
    return;
  }
  it->Next();
  data->status_ = it->status();
}

void JIterator::RunPrev(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  if (it == NULL || !it->Valid()) {
    data->invalidState_ = true;
    return;
  }
  it->Prev();
  data->status_ = it->status();
}

Handle<Value> JIterator::Op::RunSync() {
  Handle<Value> error;
  Handle<Value> result;

  Exec();
  Result(error, result);
  it_->Unlock();

  delete this;

  return error.IsEmpty() ? result : ThrowException(error);
}

void JIterator::Op::ReturnAsync() {
  HandleScope scope;

  Handle<Value> error;
  Handle<Value> result;

  Result(error, result);

  TryCatch tryCatch;

  if (error.IsEmpty()) {
    callback_->Call(it_->handle_, 0, NULL);
  } else {
    Handle<Value> argv[] = { error };
    callback_->Call(it_->handle_, 1, argv);
  }

  if (tryCatch.HasCaught()) FatalException(tryCatch);

  it_->Unlock();

  delete this;
}

async_rtn JIterator::AsyncOp(uv_work_t* req) {
  Op *op = (Op*) req->data;
  op->Exec();
  RETURN_ASYNC;
}

async_rtn JIterator::AfterOp(uv_work_t* req) {
  Op *op = (Op*) req->data;
  op->ReturnAsync();
  RETURN_ASYNC_AFTER;
}

} // node_leveldb
