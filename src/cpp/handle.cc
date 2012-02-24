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
  NODE_SET_PROTOTYPE_METHOD(constructor, "get", Read);
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

  assert(args.Length() == 2);
  assert(args[0]->IsExternal());

  leveldb::DB* db = (leveldb::DB*)External::Unwrap(args[0]);
  JHandle* self = new JHandle(db);

  if (args[1]->IsExternal())
    self->comparator_ = Persistent<Value>::New(args[1]);

  self->Wrap(args.This());

  return args.This();
}


/**

    Async support

 */

class async_params {
 public:
  virtual ~async_params() {
    callback_.Dispose();
  }

  virtual void Result(Handle<Value>& error, Handle<Value>& result) {}

  leveldb::Status status_;
  Persistent<Function> callback_;
};

static void AfterAsync(uv_work_t* req) {
  HandleScope scope;
  async_params* op = static_cast<async_params*>(req->data);
  assert(!op->callback_.IsEmpty());

  Handle<Value> error;
  Handle<Value> result;

  op->Result(error, result);

  if (error.IsEmpty() && result.IsEmpty() && !op->status_.ok())
    error = Exception::Error(String::New(op->status_.ToString().c_str()));

  if (error.IsEmpty()) error = Null();
  if (result.IsEmpty()) result = Null();

  Handle<Value> argv[] = { error, result };

  TryCatch tryCatch;
  op->callback_->Call(Context::GetCurrent()->Global(), 2, argv);
  if (tryCatch.HasCaught()) FatalException(tryCatch);

  delete op;
  delete req;
}



/**

    Open

 */

class open_params : public async_params {
 public:
  virtual ~open_params() {
    comparator_.Dispose();
  }

  virtual void Result(Handle<Value>& error, Handle<Value>& result) {
    if (status_.ok()) {
      Handle<Value> args[] = { External::New(db_), Undefined() };
      if (!comparator_.IsEmpty()) args[1] = comparator_;
      result = JHandle::constructor->GetFunction()->NewInstance(2, args);
    }
  }

  std::string name_;

  leveldb::Options options_;
  leveldb::DB* db_;

  Persistent<Value> comparator_;
};

static void AsyncOpen(uv_work_t* req) {
  open_params* op = static_cast<open_params*>(req->data);
  op->status_ = leveldb::DB::Open(op->options_, op->name_, &op->db_);
}

Handle<Value> JHandle::Open(const Arguments& args) {
  HandleScope scope;

  if (args.Length() != 3 ||
      !args[0]->IsString() ||
      !args[2]->IsFunction())
    return ThrowTypeError("Invalid arguments");

  open_params* op = new open_params;

  // Required filename
  op->name_ = *String::Utf8Value(args[0]);

  // Required callback
  op->callback_ = Persistent<Function>::New(Local<Function>::Cast(args[2]));

  // Optional options
  UnpackOptions(args[1], op->options_, &op->comparator_);

  AsyncQueue(op, AsyncOpen, AfterAsync);

  return Undefined();
}


/**

    Destroy or repair

 */

class dbop_params : public async_params {
 public:
  std::string name_;
  leveldb::Options options_;
};

static Handle<Value> DbOp(const Arguments& args, const uv_work_cb async) {
  HandleScope scope;

  if (args.Length() != 3 || !args[0]->IsString())
    return ThrowTypeError("Invalid arguments");

  dbop_params* op = new dbop_params;

  // Required filename
  op->name_ = *String::Utf8Value(args[0]);

  // Optional options
  UnpackOptions(args[1], op->options_);

  // Optional callback
  if (args[2]->IsFunction())
    op->callback_ = Persistent<Function>::New(Local<Function>::Cast(args[2]));

  AsyncQueue(op, async, AfterAsync);

  return Undefined();
}


/**

    Destroy

 */

static void AsyncDestroy(uv_work_t* req) {
  dbop_params* op = static_cast<dbop_params*>(req->data);
  op->status_ = leveldb::DestroyDB(op->name_, op->options_);
}

Handle<Value> JHandle::Destroy(const Arguments& args) {
  return DbOp(args, AsyncDestroy);
}


/**

    Repair

 */

static void AsyncRepair(uv_work_t* req) {
  dbop_params* op = static_cast<dbop_params*>(req->data);
  op->status_ = leveldb::RepairDB(op->name_, op->options_);
}

Handle<Value> JHandle::Repair(const Arguments& args) {
  return DbOp(args, AsyncRepair);
}


/**

    Read

 */

class read_params : public async_params {
 public:
  virtual ~read_params() {
    handle_.Dispose();
    keyHandle_.Dispose();
  }

  virtual void Result(Handle<Value>& error, Handle<Value>& result) {
    if (status_.ok()) {
      result = ToBuffer(result_);
    } else if (status_.IsNotFound()) {
      result = Null();
    }
  }

  leveldb::DB* db_;
  leveldb::ReadOptions options_;
  leveldb::Slice key_;

  std::string* result_;

  Persistent<Value> handle_;
  Persistent<Value> keyHandle_;
};

static void AsyncRead(uv_work_t* req) {
  read_params* op = static_cast<read_params*>(req->data);
  op->result_ = new std::string;
  op->status_ = op->db_->Get(op->options_, op->key_, op->result_);
}

Handle<Value> JHandle::Read(const Arguments& args) {
  HandleScope scope;

  if (args.Length() != 3 ||
      !Buffer::HasInstance(args[0]) ||
      !args[2]->IsFunction())
    return ThrowTypeError("Invalid arguments");

  read_params* op = new read_params;
  op->handle_ = Persistent<Value>::New(args.This());
  op->db_ = ObjectWrap::Unwrap<JHandle>(args.This())->db_;

  // Required key
  op->key_ = ToSlice(args[0], op->keyHandle_);

  // Required callback
  op->callback_ = Persistent<Function>::New(Local<Function>::Cast(args[2]));

  // Optional read options
  UnpackReadOptions(args[1], op->options_);

  AsyncQueue(op, AsyncRead, AfterAsync);

  return Undefined();
}


/**

    Write

 */

class write_params : public async_params {
 public:
  virtual ~write_params() {
    handle_.Dispose();
    batchHandle_.Dispose();
  }

  leveldb::DB* db_;
  leveldb::WriteBatch* batch_;
  leveldb::WriteOptions options_;

  Persistent<Value> handle_;
  Persistent<Value> batchHandle_;
};

static void AsyncWrite(uv_work_t* req) {
  write_params* op = static_cast<write_params*>(req->data);
  op->status_ = op->db_->Write(op->options_, op->batch_);
}

Handle<Value> JHandle::Write(const Arguments& args) {
  HandleScope scope;

  if (args.Length() != 3 ||
      !JBatch::HasInstance(args[0]) ||
      !args[2]->IsFunction())
    return ThrowTypeError("Invalid arguments");

  write_params* op = new write_params;
  op->handle_ = Persistent<Value>::New(args.This());
  op->batchHandle_ = Persistent<Value>::New(args[0]);

  // Required db and batch
  op->db_ = ObjectWrap::Unwrap<JHandle>(args.This())->db_;
  op->batch_ = &ObjectWrap::Unwrap<JBatch>(args[0]->ToObject())->wb_;

  // Optional write options
  UnpackWriteOptions(args[1], op->options_);

  // Optional callback
  op->callback_ = Persistent<Function>::New(Local<Function>::Cast(args[2]));

  AsyncQueue(op, AsyncWrite, AfterAsync);

  return Undefined();
}


/**

    Iterator

 */

class JHandle::iterator_params : public async_params {
 public:
  virtual ~iterator_params() {
    handle_.Dispose();
  }

  virtual void Result(Handle<Value>& error, Handle<Value>& result) {
    if (status_.ok()) result = self_->RefIterator(it_);
  }

  JHandle* self_;

  leveldb::Iterator* it_;
  leveldb::ReadOptions options_;

  Persistent<Value> handle_;
};

void JHandle::AsyncIterator(uv_work_t* req) {
  iterator_params* op = static_cast<iterator_params*>(req->data);
  op->it_ = op->self_->db_->NewIterator(op->options_);
}

Handle<Value> JHandle::Iterator(const Arguments& args) {
  HandleScope scope;

  if (args.Length() != 2 || !args[1]->IsFunction())
    return ThrowTypeError("Invalid arguments");

  iterator_params* op = new iterator_params;
  op->self_ = ObjectWrap::Unwrap<JHandle>(args.This());
  op->handle_ = Persistent<Value>::New(args.This());
  op->callback_ = Persistent<Function>::New(Local<Function>::Cast(args[1]));

  // Optional options
  UnpackReadOptions(args[0], op->options_);

  AsyncQueue(op, AsyncIterator, AfterAsync);

  return Undefined();
}

Handle<Value> JHandle::RefIterator(leveldb::Iterator* it) {
  Local<Value> argv[] = { External::New(it) };
  Local<Object> instance = JIterator::constructor->GetFunction()->NewInstance(1, argv);

  // Keep a weak reference
  Persistent<Object> weak = Persistent<Object>::New(instance);
  weak.MakeWeak(this, &UnrefIterator);

  Ref();

  return instance;
}

void JHandle::UnrefIterator(Persistent<Value> object, void* parameter) {
  assert(object->IsObject());

  JHandle* self = static_cast<JHandle*>(parameter);
  assert(self);

  self->Unref();
  object.Dispose();
}


/**

    Snapshot

 */

class JHandle::snapshot_params : public async_params {
 public:
  virtual ~snapshot_params() {
    handle_.Dispose();
  }

  virtual void Result(Handle<Value>& error, Handle<Value>& result) {
    if (status_.ok()) result = self_->RefSnapshot(snap_);
  }

  JHandle* self_;
  leveldb::Snapshot* snap_;
  Persistent<Value> handle_;
};

void JHandle::AsyncSnapshot(uv_work_t* req) {
  snapshot_params* op = static_cast<snapshot_params*>(req->data);
  op->snap_ = const_cast<leveldb::Snapshot*>(op->self_->db_->GetSnapshot());
}

Handle<Value> JHandle::Snapshot(const Arguments& args) {
  HandleScope scope;

  if (args.Length() != 1 || !args[0]->IsFunction())
    return ThrowTypeError("Invalid arguments");

  snapshot_params* op = new snapshot_params;
  op->self_ = ObjectWrap::Unwrap<JHandle>(args.This());
  op->handle_ = Persistent<Value>::New(args.This());
  op->callback_ = Persistent<Function>::New(Local<Function>::Cast(args[0]));

  AsyncQueue(op, AsyncSnapshot, AfterAsync);

  return Undefined();
}

Handle<Value> JHandle::RefSnapshot(const leveldb::Snapshot* snapshot) {
  Local<Value> instance = External::New((void*)snapshot);
  Persistent<Value> weak = Persistent<Value>::New(instance);
  weak.MakeWeak(this, &UnrefSnapshot);
  Ref();
  return instance;
}

void JHandle::UnrefSnapshot(Persistent<Value> object, void* parameter) {
  assert(object->IsExternal());

  JHandle* self = static_cast<JHandle*>(parameter);
  leveldb::Snapshot* snapshot =
    static_cast<leveldb::Snapshot*>(External::Unwrap(object));

  assert(self);
  assert(snapshot);

  self->db_->ReleaseSnapshot(snapshot);
  self->Unref();

  object.Dispose();
}


/**

    Property

 */

class property_params : public async_params {
 public:
  virtual ~property_params() {
    handle_.Dispose();
  }

  virtual void Result(Handle<Value>& error, Handle<Value>& result) {
    if (hasProperty_) result = String::New(value_.c_str());
  }

  leveldb::DB* db_;

  std::string name_;
  std::string value_;

  bool hasProperty_;

  Persistent<Value> handle_;
};

void AsyncProperty(uv_work_t* req) {
  property_params* op = static_cast<property_params*>(req->data);
  op->hasProperty_ = op->db_->GetProperty(op->name_, &op->value_);
}

Handle<Value> JHandle::Property(const Arguments& args) {
  HandleScope scope;

  if (args.Length() != 2 || !args[0]->IsString() || !args[1]->IsFunction())
    return ThrowTypeError("Invalid arguments");

  property_params* op = new property_params;
  op->handle_ = Persistent<Value>::New(args.This());
  op->db_ = ObjectWrap::Unwrap<JHandle>(args.This())->db_;
  op->name_ = *String::Utf8Value(args[0]);
  op->callback_ = Persistent<Function>::New(Local<Function>::Cast(args[1]));

  AsyncQueue(op, AsyncProperty, AfterAsync);

  return Undefined();
}


/**

    Approximate sizes

 */

class approxsizes_params : public async_params {
 public:
  virtual ~approxsizes_params() {
    std::vector< Persistent<Value> >::iterator it;
    for (it = handles_.begin(); it < handles_.end(); ++it) it->Dispose();
    if (sizes_) delete[] sizes_;
  }

  virtual void Result(Handle<Value>& error, Handle<Value>& result) {
    int len = ranges_.size();

    Handle<Array> array = Array::New(len);

    for (int i = 0; i < len; ++i) {
      uint64_t size = sizes_[i];
      if (size < INT_MAX) {
        array->Set(i, Integer::New(static_cast<uint32_t>(size)));
      } else {
        array->Set(i, Number::New(static_cast<double>(size)));
      }
    }

    result = array;
  }

  leveldb::DB* db_;

  std::vector<leveldb::Range> ranges_;
  std::vector< Persistent<Value> > handles_;

  uint64_t* sizes_;
};

void AsyncApproxSizes(uv_work_t* req) {
  approxsizes_params* op = static_cast<approxsizes_params*>(req->data);
  int len = op->ranges_.size();
  op->sizes_ = new uint64_t[len];
  op->db_->GetApproximateSizes(&op->ranges_[0], len, op->sizes_);
}

Handle<Value> JHandle::ApproximateSizes(const Arguments& args) {
  HandleScope scope;

  if (args.Length() != 2 || !args[0]->IsArray() || !args[1]->IsFunction())
    return ThrowTypeError("Invalid arguments");

  Local<Array> array(Array::Cast(*args[0]));

  approxsizes_params* op = new approxsizes_params;
  op->handles_.push_back(Persistent<Value>::New(args.This()));
  op->db_ = ObjectWrap::Unwrap<JHandle>(args.This())->db_;
  op->callback_ = Persistent<Function>::New(Local<Function>::Cast(args[1]));

  int len = array->Length();
  if (len % 2 != 0)
    return ThrowTypeError("Invalid arguments");

  for (int i = 0; i < len; i += 2) {
    if (array->Has(i) && array->Has(i + 1)) {
      Local<Value> lStart = array->Get(i);
      Local<Value> lLimit = array->Get(i + 1);

      leveldb::Slice start = ToSlice(lStart, op->handles_);
      leveldb::Slice limit = ToSlice(lLimit, op->handles_);

      op->ranges_.push_back(leveldb::Range(start, limit));
    }
  }

  AsyncQueue(op, AsyncApproxSizes, AfterAsync);

  return Undefined();
}

Handle<Value> JHandle::CompactRange(const Arguments& args) {
  HandleScope scope;
  return ThrowError("Method not implemented");
}

} // namespace node_leveldb
