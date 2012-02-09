#include <assert.h>
#include <pthread.h>

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
  bool valid = self->it_ != NULL && self->it_->Valid();
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
  return RunOp(&RunFirst, args);
}

Handle<Value> JIterator::Last(const Arguments& args) {
  return RunOp(&RunLast, args);
}

Handle<Value> JIterator::Next(const Arguments& args) {
  return RunOp(&RunNext, args);
}

Handle<Value> JIterator::Prev(const Arguments& args) {
  return RunOp(&RunPrev, args);
}

Handle<Value> JIterator::key(const Arguments& args) {
  return RunOp(&RunGetKey, args);
}

Handle<Value> JIterator::value(const Arguments& args) {
  return RunOp(&RunGetValue, args);
}

Handle<Value> JIterator::current(const Arguments& args) {
  return RunOp(&RunGetKeyValue, args);
}


//
// ASYNC FUNCTIONS
//


void JIterator::RunSeek(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  it->Seek(data->key_);
  data->status_ = it->status();
}

void JIterator::RunFirst(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  it->SeekToFirst();
  data->status_ = it->status();
}

void JIterator::RunLast(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  it->SeekToLast();
  data->status_ = it->status();
}

void JIterator::RunNext(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  if (it->Valid()) {
    it->Next();
    data->status_ = it->status();
  } else {
    data->invalidState_ = true;
  }
}

void JIterator::RunPrev(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  if (it->Valid()) {
    it->Prev();
    data->status_ = it->status();
  } else {
    data->invalidState_ = true;
  }
}

void JIterator::RunGetKey(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  if (it->Valid()) {
    data->value_ = it->key();
  } else {
    data->status_ = it->status();
  }
}

void JIterator::RunGetValue(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  if (it->Valid()) {
    data->value_ = it->value();
  } else {
    data->status_ = it->status();
  }
}

void JIterator::RunGetKeyValue(Op* data) {
  leveldb::Iterator* it = data->it_->it_;
  if (it->Valid()) {
    data->key_ = it->key();
    data->value_ = it->value();
  } else {
    data->status_ = it->status();
  }
}

Handle<Value> JIterator::Op::RunSync() {
  Handle<Value> error;
  Handle<Value> result = Null();

  Exec();
  it_->Unlock();
  Result(error, result);

  delete this;

  return error.IsEmpty() ? result : ThrowException(error);
}

void JIterator::Op::ReturnAsync() {
  HandleScope scope;

  it_->Unlock();

  Handle<Value> error = Null();
  Handle<Value> result = Null();

  Result(error, result);

  TryCatch tryCatch;

  Handle<Value> argv[] = { error, result };
  callback_->Call(it_->handle_, 2, argv);

  if (tryCatch.HasCaught()) FatalException(tryCatch);

  delete this;
}

void JIterator::Op::Result(Handle<Value>& error, Handle<Value>& result) {
  if (invalidState_) {
    error = Exception::Error(String::New("Illegal state"));
  } else if (!status_.ok()) {
    error = Exception::Error(String::New(status_.ToString().c_str()));
  } else if (!value_.empty()) {
    if (!key_.empty()) {
      Local<Array> array = Array::New(2);
      array->Set(0, ToBuffer(key_));
      array->Set(1, ToBuffer(value_));
      result = array;
    } else {
      result = ToBuffer(value_);
    }
  }
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
