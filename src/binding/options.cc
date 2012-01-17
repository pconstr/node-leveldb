#include <string>
#include <vector>

#include "options.h"
#include "snapshot.h"

namespace node_leveldb {

void Options::Parse(Handle<Value> val, leveldb::Options &options) {
  HandleScope scope;
  Local<Object> obj = Object::Cast(*val);

  static const Persistent<String> kCreateIfMissing = NODE_PSYMBOL("create_if_missing");
  static const Persistent<String> kErrorIfExists = NODE_PSYMBOL("error_if_exists");
  static const Persistent<String> kParanoidChecks = NODE_PSYMBOL("paranoid_checks");
  static const Persistent<String> kWriteBufferSize = NODE_PSYMBOL("write_buffer_size");
  static const Persistent<String> kMaxOpenFiles = NODE_PSYMBOL("max_open_files");
  static const Persistent<String> kBlockSize = NODE_PSYMBOL("block_size");
  static const Persistent<String> kBlockRestartInterval = NODE_PSYMBOL("block_restart_interval");

  if (obj->Has(kCreateIfMissing))
    options.create_if_missing = obj->Get(kCreateIfMissing)->BooleanValue();

  if (obj->Has(kErrorIfExists))
    options.error_if_exists = obj->Get(kErrorIfExists)->BooleanValue();

  if (obj->Has(kParanoidChecks))
    options.paranoid_checks = obj->Get(kParanoidChecks)->BooleanValue();

  if (obj->Has(kWriteBufferSize))
    options.write_buffer_size = obj->Get(kWriteBufferSize)->Int32Value();

  if (obj->Has(kMaxOpenFiles))
    options.max_open_files = obj->Get(kMaxOpenFiles)->Int32Value();

  if (obj->Has(kBlockSize))
    options.block_size = obj->Get(kBlockSize)->Int32Value();

  if (obj->Has(kBlockRestartInterval))
    options.block_restart_interval = obj->Get(kBlockRestartInterval)->Int32Value();

  // comparator
  // logger
  // compression
}

void Options::ParseForRead(Handle<Value> val, leveldb::ReadOptions &options, bool &asBuffer) {
  HandleScope scope;
  Local<Object> obj = Object::Cast(*val);

  static const Persistent<String> kVerifyChecksums = NODE_PSYMBOL("verify_checksums");
  static const Persistent<String> kFillCache = NODE_PSYMBOL("fill_cache");
  static const Persistent<String> kSnapshot = NODE_PSYMBOL("snapshot");
  static const Persistent<String> kAsBuffer = NODE_PSYMBOL("as_buffer");

  if (obj->Has(kVerifyChecksums))
    options.verify_checksums = obj->Get(kVerifyChecksums)->BooleanValue();

  if (obj->Has(kFillCache))
    options.fill_cache = obj->Get(kFillCache)->BooleanValue();

  if (obj->Has(kSnapshot)) {
    Local<Value> value = obj->Get(kSnapshot);
    if (Snapshot::HasInstance(value)) {
      Snapshot *snapshot = ObjectWrap::Unwrap<Snapshot>(value->ToObject());
      options.snapshot = **snapshot;
    }
  }

  if (obj->Has(kAsBuffer))
    asBuffer = obj->Get(kAsBuffer)->ToBoolean()->BooleanValue();

}

void Options::ParseForWrite(Handle<Value> val, leveldb::WriteOptions &options) {
  HandleScope scope;
  Local<Object> obj = Object::Cast(*val);

  static const Persistent<String> kSync = NODE_PSYMBOL("sync");

  if (obj->Has(kSync))
    options.sync = obj->Get(kSync)->BooleanValue();

}

}  // namespace node_leveldb
