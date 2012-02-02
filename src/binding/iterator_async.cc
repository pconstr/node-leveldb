#include <assert.h>

#include <leveldb/iterator.h>
#include <node.h>
#include <v8.h>

#include "iterator.h"

namespace node_leveldb {

async_rtn JIterator::After(uv_work_t* req) {
  Params *data = (Params*) req->data;
  data->callback->Call(data->self->handle_, 0, NULL);
  delete data;
  RETURN_ASYNC_AFTER;
}

async_rtn JIterator::Seek(uv_work_t* req) {
  SeekParams *data = (SeekParams*) req->data;
  data->self->it_->Seek(data->key);
  RETURN_ASYNC;
}

async_rtn JIterator::First(uv_work_t* req) {
  SeekParams *data = (SeekParams*) req->data;
  data->self->it_->SeekToFirst();
  RETURN_ASYNC;
}

async_rtn JIterator::Last(uv_work_t* req) {
  SeekParams *data = (SeekParams*) req->data;
  data->self->it_->SeekToLast();
  RETURN_ASYNC;
}

async_rtn JIterator::Next(uv_work_t* req) {
  SeekParams *data = (SeekParams*) req->data;
  data->self->it_->Next();
  RETURN_ASYNC;
}

async_rtn JIterator::Prev(uv_work_t* req) {
  SeekParams *data = (SeekParams*) req->data;
  data->self->it_->Prev();
  RETURN_ASYNC;
}

} // node_leveldb
