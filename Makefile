# This makefile wrapper makes DEBIAN packaging easier.
PREFIX ?=/usr

START = $(DESTDIR)$(PREFIX)

OCAML_LIBDIR ?= $(START)/lib/ocaml/
#OCAML_LIBDIR ?= `ocamlfind printconf destdir`
OCAML_FIND ?= ocamlfind

JOBS := $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 1)

all: build

clean:
	rm -rf ./debian/tmp/*
	rm -rf ./debian/libarakoon-ocaml-dev/*
	ocamlbuild -clean
	rm -f  ./arakoon.byte ./arakoon.native

build:
	ocamlbuild -j $(JOBS) arakoon.byte arakoon.native arakoon_client.cma arakoon_client.cmxa arakoon_client.a arakoon_client.cmxs plugin_helper.cmi

bench:
	ocamlbuild bs_bench.native

arakoon.byte arakoon.native:
	ocamlbuild -j $(JOBS) $@

test:	arakoon.native
	ocamlbuild -j $(JOBS) arakoon.native
	./arakoon.native --run-all-tests

install: install_client install_server man

install_server:
	mkdir -p $(START)/bin/
	cp ./arakoon.native $(START)/bin/arakoon

install_client:
	mkdir -p $(OCAML_LIBDIR)
	$(OCAML_FIND) install arakoon_client -destdir $(OCAML_LIBDIR) META \
          _build/src/arakoon_client.cma \
          _build/src/arakoon_client.cmxa \
          _build/src/arakoon_client.cmxs \
          _build/src/client/stamp.cmi \
	  _build/src/client/stamp.cmx \
          _build/src/client/arakoon_exc.mli \
          _build/src/client/arakoon_exc.cmi \
	  _build/src/client/arakoon_exc.cmx \
          _build/src/client/arakoon_client_config.cmi \
	  _build/src/client/arakoon_client_config.cmx \
          _build/src/client/arakoon_client.mli \
          _build/src/client/arakoon_client.cmi \
          _build/src/client/arakoon_remote_client.mli \
          _build/src/client/arakoon_remote_client.cmi \
	  _build/src/client/arakoon_remote_client.cmx \
          _build/src/client/protocol_common.cmi \
	  _build/src/client/protocol_common.cmx \
          _build/src/client/client_helper.cmi \
	  _build/src/client/client_helper.cmx \
          _build/src/inifiles/arakoon_inifiles.mli \
          _build/src/inifiles/arakoon_inifiles.cmi \
	  _build/src/inifiles/arakoon_inifiles.cmx \
          _build/src/lib/std.cmi \
          _build/src/lib/std.cmx \
          _build/src/plugins/registry.mli \
          _build/src/plugins/registry.cmi \
	  _build/src/plugins/registry.cmx \
          _build/src/plugins/plugin_helper.mli \
          _build/src/plugins/plugin_helper.cmi \
	  _build/src/plugins/plugin_helper.cmx \
          _build/src/node/key.mli \
          _build/src/node/key.cmi \
	  _build/src/node/key.cmx \
          _build/src/node/simple_store.cmi \
	  _build/src/node/simple_store.cmx \
          _build/src/nursery/routing.cmi \
          _build/src/tlog/arakoon_interval.cmi \
          _build/src/tlog/arakoon_interval.cmx \
          _build/src/tlog/update.mli \
          _build/src/tlog/update.cmi \
	  _build/src/tlog/update.cmx \
          _build/src/tools/ini.cmi \
          _build/src/tools/ini.cmx \
          _build/src/tools/arakoon_log_sink.cmi \
	  _build/src/tools/arakoon_log_sink.cmx \
          _build/src/tools/crash_logger.cmi \
	  _build/src/tools/crash_logger.cmx \
          _build/src/tools/arakoon_redis.cmi \
          _build/src/tools/arakoon_redis.cmx \
          _build/src/tools/arakoon_logger.cmi \
	  _build/src/tools/arakoon_logger.cmx \
          _build/src/tools/llio.mli \
          _build/src/tools/llio.cmi \
	  _build/src/tools/llio.cmx \
          _build/src/tools/network.cmi \
	  _build/src/tools/network.cmx \
          _build/src/tools/lwt_buffer.cmi \
	  _build/src/tools/lwt_buffer.cmx \
          _build/src/tools/lwt_extra.cmi \
	  _build/src/tools/lwt_extra.cmx \
          _build/src/tools/typed_ssl.mli \
          _build/src/tools/typed_ssl.cmi \
	  _build/src/tools/typed_ssl.cmx \
          _build/src/tools/unix_fd.cmi \
          _build/src/tools/unix_fd.cmx \
          _build/src/tools/arakoon_etcd.cmi \
	  _build/src/tools/arakoon_etcd.cmx \
          _build/src/tools/arakoon_config_url.cmi \
	  _build/src/tools/arakoon_config_url.cmx \
          _build/src/tools/tcp_keepalive_sockopt_stubs.o \
          _build/src/tools/tcp_keepalive.cmi \
	  _build/src/tools/tcp_keepalive.cmx \
          _build/src/arakoon_client.a

uninstall_client:
	$(OCAML_FIND) remove arakoon_client -destdir $(OCAML_LIBDIR)

coverage:
	ocamlbuild -tag use_bisect arakoon.d.byte

man:
	ln -s ./arakoon.native arakoon
	help2man --name='Arakoon, a consistent key value store' ./arakoon > debian/arakoon.man
	rm arakoon

.PHONY: install test build install_client bench arakoon.byte arakoon.native


indent-tabs-to-spaces:
	@echo "Converting tabs to spaces..."
	@find . -iname '*.ml' -o -iname '*.mli' -exec sed -i -e 's/\t/  /g' {} \;

indent-trailing-whitespace:
	@echo "Removing trailing whitespace..."
	@find . -iname '*.ml' -o -iname '*.mli' -exec sed -i -e 's/[[:space:]]*$$//' {} \;

indent-trailing-lines:
	@echo "Removing trailing newlines..."
	@find . -iname '*.ml' -o -iname '*.mli' -exec sed -i -e :a -e '/^\n*$$/{$$d;N;ba' -e '}' {} \;

indent-ocp-indent:
	@echo "Running ocp-indent..."
	@find . -iname '*.ml' -o -iname '*.mli' -exec ocp-indent -i {} \;

indent: indent-tabs-to-spaces indent-trailing-whitespace indent-trailing-lines indent-ocp-indent
	@echo "Done"

.PHONY: indent-tabs-to-spaces indent-trailing-whitespace indent-trailing-lines indent-ocp-indent indent
