#include <assert.h>

#include <leveldb/iterator.h>
#include <node.h>
#include <pthread.h>
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

static inline Handle<Value> ToJS(leveldb::Status& status) {
  if (!status.ok()) return String::New(status.ToString().c_str());
  return Undefined();
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
  return self->Valid() ? True() : False();
}

Handle<Value> JIterator::Seek(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  if (args.Length() < 1 || !Buffer::HasInstance(args[0]))
    return ThrowTypeError("Invalid arguments");

  leveldb::Slice key = ToSlice(args[0]);

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    leveldb::Status status;
    if (self->Seek(key, status)) {
      return ThrowError("Illegal state");
    } else {
      return ToJS(status);
    }
  } else {
    SeekParams *data = new SeekParams(
      self, key, Persistent<Value>::New(args[0]), callback);
    BEGIN_ASYNC(data, Seek, After);
  }

  return Undefined();
}

Handle<Value> JIterator::First(const Arguments& args) {
  HandleScope scope;

  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    leveldb::Status status;
    if (self->First(status)) {
      return ThrowError("Illegal state");
    } else {
      return ToJS(status);
    }
  } else {
    Params *data = new Params(self, callback);
    BEGIN_ASYNC(data, First, After);
  }

  return Undefined();
}

Handle<Value> JIterator::Last(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    leveldb::Status status;
    if (self->Last(status)) {
      return ThrowError("Illegal state");
    } else {
      return ToJS(status);
    }
  } else {
    Params *data = new Params(self, callback);
    BEGIN_ASYNC(data, Last, After);
  }

  return Undefined();
}

Handle<Value> JIterator::Next(const Arguments& args) {
  HandleScope scope;
  JIterator* self = ObjectWrap::Unwrap<JIterator>(args.This());

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    leveldb::Status status;
    if (self->Next(status)) {
      return ThrowError("Illegal state");
    } else {
      return ToJS(status);
    }
  } else {
    Params *data = new Params(self, callback);
    BEGIN_ASYNC(data, Next, After);
  }

  return Undefined();
}

Handle<Value> JIterator::Prev(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  // Optional callback
  Local<Function> callback = GetCallback(args);

  if (callback.IsEmpty()) {
    leveldb::Status status;
    if (self->Prev(status)) {
      return ThrowError("Illegal state");
    } else {
      return ToJS(status);
    }
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

Handle<Value> JIterator::key(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  bool asBuffer = AsBuffer(args);

  leveldb::Slice val;
  if (self->key(val)) return Undefined();

  if (asBuffer) {
    return scope.Close(ToBuffer(val));
  } else {
    return scope.Close(ToString(val));
  }
}

Handle<Value> JIterator::value(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  bool asBuffer = AsBuffer(args);

  leveldb::Slice val;
  if (self->value(val)) return Undefined();

  if (asBuffer) {
    return scope.Close(ToBuffer(val));
  } else {
    return scope.Close(ToString(val));
  }
}

Handle<Value> JIterator::current(const Arguments& args) {
  HandleScope scope;
  JIterator *self = ObjectWrap::Unwrap<JIterator>(args.This());

  bool asBuffer = AsBuffer(args);

  leveldb::Slice key;
  leveldb::Slice val;

  if (self->current(key, val)) return Undefined();

  Local<Array> pair = Array::New(2);

  if (asBuffer) {
    pair->Set(0, ToBuffer(key));
    pair->Set(1, ToBuffer(val));
  } else {
    pair->Set(0, ToString(key));
    pair->Set(1, ToString(val));
  }

  return scope.Close(pair);
}


//
// ASYNC FUNCTIONS
//

async_rtn JIterator::After(uv_work_t* req) {
  Params *data = (Params*) req->data;

  TryCatch try_catch;

  if (data->error) {
    Handle<Value> argv[] = { Exception::Error(String::New("Illegal state")) };
    data->callback->Call(data->self->handle_, 1, argv);
  } else if (!data->status.ok()) {
    Local<String> msg = String::New(data->status.ToString().c_str());
    Handle<Value> argv[] = { Exception::Error(msg) };
    data->callback->Call(data->self->handle_, 1, argv);
  } else {
    data->callback->Call(data->self->handle_, 0, NULL);
  }

  if (try_catch.HasCaught()) FatalException(try_catch);

  delete data;
  RETURN_ASYNC_AFTER;
}

async_rtn JIterator::Seek(uv_work_t* req) {
  SeekParams *data = (SeekParams*) req->data;
  data->error = data->self->Seek(data->key, data->status);
  RETURN_ASYNC;
}

async_rtn JIterator::First(uv_work_t* req) {
  Params *data = (Params*) req->data;
  data->error = data->self->First(data->status);
  RETURN_ASYNC;
}

async_rtn JIterator::Last(uv_work_t* req) {
  Params *data = (Params*) req->data;
  data->error = data->self->Last(data->status);
  RETURN_ASYNC;
}

async_rtn JIterator::Next(uv_work_t* req) {
  Params *data = (Params*) req->data;
  data->error = data->self->Next(data->status);
  RETURN_ASYNC;
}

async_rtn JIterator::Prev(uv_work_t* req) {
  Params *data = (Params*) req->data;
  data->error = data->self->Prev(data->status);
  RETURN_ASYNC;
}

} // node_leveldb
