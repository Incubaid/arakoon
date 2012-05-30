# This makefile wrapper makes DEBIAN packaging easier. 

OCAML_VERSION ?= 3.12.1
OCAML_LIBDIR ?= $(DESTDIR)/usr/lib/ocaml/
OCAML_FIND ?= ocamlfind

all: build

clean:
	ocamlbuild -clean

build:
	ocamlbuild -use-ocamlfind arakoon.cmxa barakoon.native

test:
	./barakoon.native --test

install: install_lib install_server

install_server:
	mkdir -p $(DESTDIR)/usr/bin/
	cp ./barakoon.native $(DESTDIR)/usr/bin/arakoon

install_lib:
	mkdir -p $(OCAML_LIBDIR)
	$(OCAML_FIND) install arakoon META \
	  _build/src/hope/arakoon.cmxa \
          _build/src/hope/core.cmi \
	  _build/src/hope/userdb.cmi 


uninstall_lib:
	$(OCAML_FIND) remove arakoon 

coverage:
	ocamlbuild -use-ocamlfind \
	-tag 'package(bisect)' \
	-tag 'syntax(camlp4o)' \
	-tag 'syntax(bisect_pp)' \
	barakoon.d.byte

.PHONY: install test build install_lib
         
