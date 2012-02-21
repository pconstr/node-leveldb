REPORTER=dot
BINARY=./lib/leveldb.node

build:
	if [ ! -f ./deps/snappy/Makefile ]; then node-waf configure; fi
	node-waf build

coffee:
	if [ -f ./node_modules/.bin/coffee ]; then \
		./node_modules/.bin/coffee --bare --compile --output lib src; \
	else \
		coffee --bare --compile --output lib src; \
	fi

clean:
	node-waf clean

distclean: clean
	rm -rf lib

test: build coffee
	mkdir -p tmp
	-@./node_modules/.bin/mocha --reporter $(REPORTER) test/*-test.coffee
	rm -rf tmp

.PHONY: build coffee clean distclean test
