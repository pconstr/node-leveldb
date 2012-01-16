#include <node.h>
#include <v8.h>

#include "helpers.h"

using namespace v8;

namespace node_leveldb {

leveldb::Slice JsToSlice(Handle<Value> val, std::vector<std::string> *strings) {
  HandleScope scope;
  if (val->IsString() && strings != NULL) {
    std::string str(*String::Utf8Value(val));
    strings->push_back(str);
    return leveldb::Slice(str.data(), str.length());
  }
  else if (Buffer::HasInstance(val)) {
    Local<Object> obj = Object::Cast(*val);
    return leveldb::Slice(BufferData(obj), BufferLength(obj));
  }
  else {
    return leveldb::Slice();
  }
}

Handle<Value> processStatus(leveldb::Status status) {
  static const Persistent<String> kOK = NODE_PSYMBOL("OK");
  if (status.ok()) return kOK;
  return ThrowError(status.ToString().c_str());
}

Local<Object> Bufferize(std::string value) {
  HandleScope scope;

  int length = value.length();
  Buffer *slowBuffer = Buffer::New(length);
  memcpy(Buffer::Data(slowBuffer), value.c_str(), length);

  Local<Object> global = v8::Context::GetCurrent()->Global();
  Local<Value> bv = global->Get(String::NewSymbol("Buffer"));
  assert(bv->IsFunction());
  Local<Function> b = Local<Function>::Cast(bv);
  Handle<Value> argv[3] = { slowBuffer->handle_, Integer::New(length), Integer::New(0) };
  Local<Object> instance = b->NewInstance(3, argv);

  return scope.Close(instance);
}

char* BufferData(Buffer *b) {
  return Buffer::Data(b->handle_);
}

size_t BufferLength(Buffer *b) {
  return Buffer::Length(b->handle_);
}

char* BufferData(Handle<Object> buf_obj) {
  return Buffer::Data(buf_obj);
}

size_t BufferLength(Handle<Object> buf_obj) {
  return Buffer::Length(buf_obj);
}

} // node_leveldb
