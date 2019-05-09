# This makefile wrapper makes DEBIAN packaging easier.

# The debian installer defined DESTDIR. If set, we install where debian expects it,
# othervice we install where opam expects it
ifdef DESTDIR
  LIBDIR=$(DESTDIR)/usr/lib/ocaml
  PREFIX=$(DESTDIR)/usr
else
  PREFIX=$(shell opam config var prefix)
  LIBDIR=$(shell opam config var lib)
endif

all: arakoon.native bench.native lwt_buffer_test.native

build:
	@dune build

arakoon.native: build
	@ln -sf _build/default/src/main/arakoon.exe arakoon.native

bench.native:
	@dune build src/main/bs_bench.exe
	@ln -sf _build/default/src/main/bs_bench.exe bs_bench.native

lwt_buffer_test.native:
	@dune build src/main/lwt_buffer_test.exe
	@ln -sf _build/default/src/main/lwt_buffer_test.exe lwt_buffer_test.native

clean:
	dune clean
	rm -f arakoon.native bs_bench.native lwt_buffer_test.native

test:	build
	dune exec -- arakoon test

install: build
	@mkdir -p $(LIBDIR) $(PREFIX)
ifdef DESTDIR
	mkdir -p $(DESTDIR)/usr/lib
	ldd arakoon.native | \
          grep librocksdb | \
          awk '/=> /{print $$3}' | \
          xargs cp --target-directory $(DESTDIR)/usr/lib
endif
	dune install --libdir=$(LIBDIR) --prefix=$(PREFIX)

uninstall: build
	dune uninstall --libdir=$(LIBDIR) --prefix=$(PREFIX)

uninstall_client: build
	dune uninstall -p arakoon_client --libdir=$(LIBDIR) --prefix=$(PREFIX)

man:
	ln -s ./arakoon.native arakoon
	help2man --name='Arakoon, a consistent key value store' ./arakoon > debian/arakoon.man
	rm arakoon

examples:
	dune build examples/ocaml/plugin_demo.exe examples/ocaml/demo.exe

doc:
	dune build @doc


.PHONY: install uninstall test build bench examples arakoon.native doc man



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
