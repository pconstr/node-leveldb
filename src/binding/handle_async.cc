#include <string>
#include <vector>

#include <leveldb/db.h>
#include <node.h>
#include <v8.h>

#include "handle.h"
#include "helpers.h"
#include "node_async_shim.h"

using namespace node;
using namespace v8;

namespace node_leveldb {

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
    Handle<String> message = String::New(status.ToString().c_str());
    Handle<Value> argv[] = { Exception::Error(message) };
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
    Handle<Object> self = constructor->GetFunction()->NewInstance(1, argc);
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
  params->status = params->self->db_->Get(params->options, params->slice, &params->result);
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
