#include <assert.h>

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

  NODE_SET_PROTOTYPE_METHOD(constructor, "first", First);
  NODE_SET_PROTOTYPE_METHOD(constructor, "last", Last);
  NODE_SET_PROTOTYPE_METHOD(constructor, "seek", Seek);
  NODE_SET_PROTOTYPE_METHOD(constructor, "next", Next);
  NODE_SET_PROTOTYPE_METHOD(constructor, "prev", Prev);
  NODE_SET_PROTOTYPE_METHOD(constructor, "key", GetKey);
  NODE_SET_PROTOTYPE_METHOD(constructor, "current", GetKeyValue);
}

Handle<Value> JIterator::New(const Arguments& args) {
  HandleScope scope;

  assert(args.Length() == 1);
  assert(args[0]->IsExternal());

  leveldb::Iterator* it =
    static_cast<leveldb::Iterator*>(External::Unwrap(args[0]));

  assert(it);

  JIterator* iterator = new JIterator(it);
  iterator->Wrap(args.This());

  return args.This();
}





class JIterator::iter_params {
 public:
  iter_params(const Arguments& args) : valid_(false) {
    self_ = ObjectWrap::Unwrap<JIterator>(args.This());
    self_->Ref();
    assert(args[args.Length() - 1]->IsFunction());
    Local<Function> fn = Local<Function>::Cast(args[args.Length() - 1]);
    callback_ = Persistent<Function>::New(fn);
  }

  virtual ~iter_params() {
    self_->Unref();
    callback_.Dispose();
  }

  virtual void Result(Handle<Value>& result) {
    result = valid_ ? True() : False();
  }

  JIterator* self_;
  leveldb::Status status_;
  Persistent<Function> callback_;
  bool valid_;
};

class JIterator::seek_params : public iter_params {
 public:
  seek_params(const Arguments& args) : iter_params(args) {}
  ~seek_params() { keyHandle_.Dispose(); }
  leveldb::Slice key_;
  Persistent<Value> keyHandle_;
};

class JIterator::kv_params : public iter_params {
 public:
  kv_params(const Arguments& args) : iter_params(args) {}

  virtual void Result(Handle<Value>& result) {
    if (!value_.empty()) {
      Handle<Array> array = Array::New(2);
      array->Set(0, ToBuffer(key_));
      array->Set(1, ToBuffer(value_));
      result = array;
    } else {
      result = ToBuffer(key_);
    }
  }

  leveldb::Slice key_;
  leveldb::Slice value_;
};

void JIterator::AfterAsync(uv_work_t* req) {
  HandleScope scope;
  iter_params* op = static_cast<iter_params*>(req->data);
  assert(!op->callback_.IsEmpty());

  Handle<Value> error = Null();
  Handle<Value> result = Null();

  if (!op->status_.ok()) {
    error = Exception::Error(String::New(op->status_.ToString().c_str()));
  } else {
    op->Result(result);
  }

  TryCatch tryCatch;
  Handle<Value> args[] = { error, result };
  op->callback_->Call(Context::GetCurrent()->Global(), 2, args);
  if (tryCatch.HasCaught()) FatalException(tryCatch);

  delete op;
  delete req;
}





Handle<Value> JIterator::Seek(const Arguments& args) {
  HandleScope scope;

  // Required key
  if (args.Length() < 1 || !Buffer::HasInstance(args[0]))
    return ThrowTypeError("Invalid arguments");

  seek_params* op = new seek_params(args);
  op->key_ = ToSlice(args[0], op->keyHandle_);

  AsyncQueue(op, AsyncSeek, AfterAsync);
  return Undefined();
}

void JIterator::AsyncSeek(uv_work_t* req) {
  seek_params* op = static_cast<seek_params*>(req->data);
  op->self_->it_->Seek(op->key_);
  op->status_ = op->self_->it_->status();
  op->valid_ = op->self_->it_->Valid();
}





Handle<Value> JIterator::First(const Arguments& args) {
  iter_params* op = new iter_params(args);
  AsyncQueue(op, AsyncFirst, AfterAsync);
  return Undefined();
}

void JIterator::AsyncFirst(uv_work_t* req) {
  iter_params* op = static_cast<iter_params*>(req->data);
  op->self_->it_->SeekToFirst();
  op->status_ = op->self_->it_->status();
  op->valid_ = op->self_->it_->Valid();
}





Handle<Value> JIterator::Last(const Arguments& args) {
  iter_params* op = new iter_params(args);
  AsyncQueue(op, AsyncLast, AfterAsync);
  return Undefined();
}

void JIterator::AsyncLast(uv_work_t* req) {
  iter_params* op = static_cast<iter_params*>(req->data);
  op->self_->it_->SeekToLast();
  op->status_ = op->self_->it_->status();
  op->valid_ = op->self_->it_->Valid();
}





Handle<Value> JIterator::Next(const Arguments& args) {
  iter_params* op = new iter_params(args);
  AsyncQueue(op, AsyncNext, AfterAsync);
  return Undefined();
}

void JIterator::AsyncNext(uv_work_t* req) {
  iter_params* op = static_cast<iter_params*>(req->data);
  op->self_->it_->Next();
  op->status_ = op->self_->it_->status();
  op->valid_ = op->self_->it_->Valid();
}





Handle<Value> JIterator::Prev(const Arguments& args) {
  iter_params* op = new iter_params(args);
  AsyncQueue(op, AsyncPrev, AfterAsync);
  return Undefined();
}

void JIterator::AsyncPrev(uv_work_t* req) {
  iter_params* op = static_cast<iter_params*>(req->data);
  op->self_->it_->Prev();
  op->status_ = op->self_->it_->status();
  op->valid_ = op->self_->it_->Valid();
}





Handle<Value> JIterator::GetKey(const Arguments& args) {
  kv_params* op = new kv_params(args);
  AsyncQueue(op, AsyncGetKey, AfterAsync);
  return Undefined();
}

void JIterator::AsyncGetKey(uv_work_t* req) {
  kv_params* op = static_cast<kv_params*>(req->data);
  op->key_ = op->self_->it_->key();
}





Handle<Value> JIterator::GetKeyValue(const Arguments& args) {
  kv_params* op = new kv_params(args);
  AsyncQueue(op, AsyncGetKeyValue, AfterAsync);
  return Undefined();
}

void JIterator::AsyncGetKeyValue(uv_work_t* req) {
  kv_params* op = static_cast<kv_params*>(req->data);
  op->key_ = op->self_->it_->key();
  op->value_ = op->self_->it_->value();
}

} // node_leveldb
