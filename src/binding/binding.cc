#include <node.h>

#include "handle.h"
#include "batch.h"
#include "iterator.h"
//#include "snapshot.h"

namespace node_leveldb {

extern "C" {

static void init(Handle<Object> target) {
  JHandle::Initialize(target);
  JBatch::Initialize(target);
  JIterator::Initialize(target);
}

NODE_MODULE(leveldb, init);

} // extern

} // namespace node_leveldb
