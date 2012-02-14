BINARY=./lib/leveldb.node
BUILD_CONFIG=./deps/leveldb/build_config.mk
SOURCES=\
	./src/binding \
	./src/binding/batch.cc \
	./src/binding/batch.h \
	./src/binding/binding.cc \
	./src/binding/handle.cc \
	./src/binding/handle.h \
	./src/binding/helpers.h \
	./src/binding/iterator.cc \
	./src/binding/iterator.h \
	./src/binding/node_async_shim.h \
	./src/binding/operation.h \
	./src/binding/options.h

COFFEE=./node_modules/.bin/coffee
MOCHA=./node_modules/.bin/mocha

build: configure $(BINARY)

configure: $(BUILD_CONFIG)

coffee:
	$(COFFEE) --bare --compile --output lib src

clean:
	node-waf clean

distclean: clean
	rm -f lib/leveldb.node

test: $(BINARY) coffee
	mkdir -p tmp
	-find tests -name '*-test.*' -print0 | xargs -0 $(MOCHA) -R spec
	rm -rf tmp

$(BUILD_CONFIG):
	node-waf configure

$(BINARY): $(SOURCES)
	node-waf build

.PHONY: build coffee configure clean distclean test
