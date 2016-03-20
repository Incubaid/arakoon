#!/bin/bash -xue

APT_DEPENDS="libev-dev libssl-dev libsnappy-dev libgmp3-dev"
APT_OCAML_DEPENDS="ocaml ocaml-native-compilers camlp4-extra opam"
OPAM_DEPENDS="ssl.0.5.2 \
              conf-libev \
              lwt.2.5.1 \
              camlbz2 \
              camltc.999 \
              snappy \
              quickcheck \
              ocplib-endian \
              redis.0.2.3 \
              uri.1.9.1 \
              core.113.00.00
"

export OPAMYES=1
export OPAMVERBOSE=1

before_install () {
    echo "Running 'before_install' phase"

    echo "Adding PPA '$PPA'"
    echo "yes" | sudo add-apt-repository ppa:$PPA
    echo "Updating Apt cache"
    sudo apt-get update -qq
    echo "updating keys"
    sudo apt-key update
    echo "Installing general dependencies"
    sudo apt-get install -qq ${APT_DEPENDS}
    echo "Installing dependencies"
    sudo apt-get install -qq ${APT_OCAML_DEPENDS}

    echo "OCaml versions:"
    ocaml -version
    ocamlopt -version

    echo "Opam versions:"
    opam --version
    opam --git-version
}

install () {
    echo "Running 'install' phase"

    opam init
    eval `opam config env`

    opam remote add incubaid-devel -k git git://github.com/Incubaid/opam-repository-devel.git
    opam update

    opam install ${OPAM_DEPENDS}

    # Install etcd:
    curl -L  https://github.com/coreos/etcd/releases/download/v2.2.4/etcd-v2.2.4-linux-amd64.tar.gz -o etcd-v2.2.4-linux-amd64.tar.gz
    tar xzvf etcd-v2.2.4-linux-amd64.tar.gz
    sudo cp ./etcd-v2.2.4-linux-amd64/etcd /usr/bin
    sudo cp ./etcd-v2.2.4-linux-amd64/etcdctl /usr/bin
}

script () {
    echo "Running 'script' phase"
    eval `opam config env`

    echo "Building 'arakoon.native'"
    ocamlbuild -use-ocamlfind -classic-display arakoon.native

    echo "Arakoon version:"
    ./arakoon.native --version

    echo "Executing tests"
    ./arakoon.native --run-all-tests 2>&1 | tail -n256
    exit ${PIPESTATUS[0]}
}

case "$1" in
    before_install)
        before_install
        ;;
    install)
        install
        ;;
    script)
        script
        ;;
    *)
        echo "Usage: $0 {before_install|install|script}"
        exit 1
esac
