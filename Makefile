# This makefile wrapper makes DEBIAN packaging easier.
PREFIX ?=/usr
OCAML_VERSION ?= 4.00.1

START = $(DESTDIR)$(PREFIX)
OCAML_LIBDIR ?= $(START)/lib/ocaml/
OCAML_FIND ?= ocamlfind

all: build

clean:
	ocamlbuild -clean

build:
	ocamlbuild -j 4 -use-ocamlfind arakoon.byte arakoon.native arakoon_client.cma arakoon_client.cmxa arakoon_client.a arakoon_client.cmxs

test:
	./arakoon.native --run-all-tests

install: install_client install_server

install_server: man
	mkdir -p $(START)/bin/
	cp ./arakoon.native $(START)/bin/arakoon

install_client:
	mkdir -p $(OCAML_LIBDIR)
	$(OCAML_FIND) install arakoon_client -destdir $(OCAML_LIBDIR) META \
	  _build/src/arakoon_client.cma \
	  _build/src/arakoon_client.cmxa \
	  _build/src/arakoon_client.cmxs \
	  _build/src/client/arakoon_exc.mli \
	  _build/src/client/arakoon_exc.cmi \
	  _build/src/client/arakoon_client.mli \
	  _build/src/client/arakoon_client.cmi \
	  _build/src/client/arakoon_remote_client.mli \
	  _build/src/client/arakoon_remote_client.cmi \
          _build/src/plugins/registry.mli \
          _build/src/plugins/registry.cmi \
          _build/src/tools/llio.mli \
          _build/src/tools/llio.cmi \
          _build/src/arakoon_client.a

uninstall_client:
	$(OCAML_FIND) remove arakoon_client -destdir $(OCAML_LIBDIR)

coverage:
	ocamlbuild -use-ocamlfind \
	-tag 'package(bisect)' \
	-tag 'syntax(camlp4o)' \
	-tag 'syntax(bisect_pp)' \
	arakoon.d.byte

man:
	help2man --name='Arakoon, a consistent key value store' ./arakoon > debian/arakoon.man

.PHONY: install test build install_client


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
