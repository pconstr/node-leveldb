BUILD_CONFIG=deps/leveldb/build_config.mk

COFFEE=./node_modules/.bin/coffee
MOCHA=./node_modules/.bin/mocha

build: configure
	node-waf build

configure: $(BUILD_CONFIG)

coffee:
	$(COFFEE) --bare --compile --output lib src

clean:
	node-waf clean

npmclean: clean
	rm -rf deps

test: build coffee
	mkdir -p tmp
	-find tests -name '*-test.*' -print0 | xargs -0 $(MOCHA) -R spec
	rm -rf tmp

$(BUILD_CONFIG):
	node-waf configure

.PHONY: build coffee configure clean npmclean test
