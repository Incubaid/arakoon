# This makefile wrapper makes DEBIAN packaging easier. 

OCAML_VERSION ?= 3.12.1
OCAML_LIBDIR ?= $(DESTDIR)/usr/lib/ocaml/
OCAML_FIND ?= ocamlfind

all: build

clean:
	ocamlbuild -clean

build:
	ocamlbuild -use-ocamlfind barakoon.native

test:
	./barakoon.native --test

install: install_client install_server

install_server:
	mkdir -p $(DESTDIR)/usr/bin/
	cp ./barakoon.native $(DESTDIR)/usr/bin/arakoon

install_client:
	mkdir -p $(OCAML_LIBDIR)
	$(OCAML_FIND) install arakoon_client -destdir $(OCAML_LIBDIR) META \
	  _build/src/client/arakoon_exc.mli \
	  _build/src/client/arakoon_exc.cmi \
	  _build/src/client/arakoon_client.mli \
	  _build/src/client/arakoon_client.cmi \
	  _build/src/client/arakoon_remote_client.mli \
	  _build/src/client/arakoon_remote_client.cmi

uninstall_client:
	$(OCAML_FIND) remove arakoon_client -destdir $(OCAML_LIBDIR)

coverage:
	ocamlbuild -use-ocamlfind \
	-tag 'package(bisect)' \
	-tag 'syntax(camlp4o)' \
	-tag 'syntax(bisect_pp)' \
	barakoon.d.byte

.PHONY: install test build install_client
         
