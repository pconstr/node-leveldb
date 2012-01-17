#include <assert.h>

#include <iostream>

#include <leveldb/db.h>
#include <node.h>
#include <v8.h>

#include "DB.h"
#include "helpers.h"
#include "snapshot.h"

using namespace std;

namespace node_leveldb {

Persistent<FunctionTemplate> Snapshot::constructor;

Snapshot::~Snapshot() { Close(); }

void Snapshot::Close() {
  if (!db_.IsEmpty()) {
    db()->ReleaseSnapshot(snapshot);
    db_.Dispose();
    db_.Clear();
  }
}

bool Snapshot::Valid() {
  return !db_.IsEmpty();
}

const leveldb::Snapshot* Snapshot::operator*() {
  return Valid() ? snapshot : NULL;
}

void Snapshot::Init(Handle<Object> target) {
  HandleScope scope;

  Local<FunctionTemplate> t = FunctionTemplate::New(New);
  constructor = Persistent<FunctionTemplate>::New(t);
  constructor->InstanceTemplate()->SetInternalFieldCount(1);
  constructor->SetClassName(String::NewSymbol("Snapshot"));

  NODE_SET_PROTOTYPE_METHOD(constructor, "valid", Valid);
  NODE_SET_PROTOTYPE_METHOD(constructor, "close", Close);

  target->Set(String::NewSymbol("Snapshot"), constructor->GetFunction());
}

bool Snapshot::HasInstance(Handle<Value> value) {
  return value->IsObject() && constructor->HasInstance(value->ToObject());
}

//
// Constructor
//

Handle<Value> Snapshot::New(const Arguments& args) {
  HandleScope scope;

  assert(args.Length() >= 2);
  assert(DB::HasInstance(args[0]));

  Local<Object> self = args.This();
  Local<Object> dbRef = args[0]->ToObject();

  leveldb::DB* db = **ObjectWrap::Unwrap<DB>(dbRef);

  Snapshot* snapshot = new Snapshot(db->GetSnapshot());
  snapshot->db_ = Persistent<Object>::New(dbRef);
  snapshot->Wrap(self);

  return self;
}


//
// Valid
//

Handle<Value> Snapshot::Valid(const Arguments& args) {
  HandleScope scope;

  Snapshot* self = ObjectWrap::Unwrap<Snapshot>(args.This());

  return self->Valid() ? True() : False();
}


//
// Close
//

Handle<Value> Snapshot::Close(const Arguments& args) {
  HandleScope scope;

  Snapshot* self = ObjectWrap::Unwrap<Snapshot>(args.This());
  self->Close();

  return args.This();
}

}  // namespace node_leveldb
