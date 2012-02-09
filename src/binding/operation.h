#ifndef NODE_LEVELDB_OP_H_
#define NODE_LEVELDB_OP_H_

#include <node.h>
#include <v8.h>

#include "node_async_shim.h"

using namespace v8;
using namespace node;

namespace node_leveldb {

template < class T, class S > class Operation {
 public:

  typedef void (*ExecFunction)(S* op);
  typedef void (*ConvFunction)(
    S* op, Handle<Value>& error, Handle<Value>& result);

  inline Operation(ExecFunction exec, ConvFunction conv,
                   Handle<Object>& self, Handle<Function>& callback)
    : exec_(exec), conv_(conv)
  {
    self_ = ObjectWrap::Unwrap<T>(self);
    handle_ = Persistent<Object>::New(self);
    callback_ = Persistent<Function>::New(callback);
  }

  virtual ~Operation() {
    handle_.Dispose();
    callback_.Dispose();
  }

  inline Handle<Value> Before() { return Handle<Value>(); }
  inline void After() {}

  inline Handle<Value> Run() {
    Handle<Value> error = Before();

    if (!error.IsEmpty()) {

      delete this;
      return error;

    } else if (callback_.IsEmpty()) {

      Handle<Value> result = Null();

      S* self = static_cast<S*>(this);
      exec_(self);
      conv_(self, error, result);
      After();

      delete this;

      return error.IsEmpty() ? result : ThrowException(error);

    } else {

      BEGIN_ASYNC(this, Async, After);
      return Undefined();

    }
  }

  static inline Handle<Value> Run(
    ExecFunction run, ConvFunction conv, const Arguments& args)
  {
    HandleScope scope;
    return New(run, conv, args)->Run();
  }

  static inline S* New(
    ExecFunction run, ConvFunction conv, const Arguments& args)
  {
    // Self object
    Handle<Object> self = args.This();

    // Optional callback
    Handle<Function> callback = GetCallback(args);

    // Build operation
    S* op = new S(run, conv, self, callback);

    return op;
  }

  static async_rtn Async(uv_work_t* req) {
    Operation* op = static_cast<Operation*>(req->data);
    S* self = static_cast<S*>(req->data);
    op->exec_(self);
    RETURN_ASYNC;
  }

  static async_rtn After(uv_work_t* req) {
    HandleScope scope;

    Handle<Value> error = Null();
    Handle<Value> result = Null();

    Operation* op = static_cast<Operation*>(req->data);
    S* self = static_cast<S*>(req->data);

    op->conv_(self, error, result);

    TryCatch tryCatch;

    Handle<Value> argv[] = { error, result };
    op->callback_->Call(op->handle_, 2, argv);

    if (tryCatch.HasCaught()) FatalException(tryCatch);

    delete op;

    RETURN_ASYNC_AFTER;
  }

  ExecFunction exec_;
  ConvFunction conv_;

  T* self_;

  Persistent<Object> handle_;
  Persistent<Function> callback_;
};

} // node_leveldb

#endif // NODE_LEVELDB_OP_H_
