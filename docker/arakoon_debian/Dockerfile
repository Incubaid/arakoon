FROM ubuntu:15.04
RUN apt-get -y update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
        build-essential m4 apt-utils \
        libffi-dev libssl-dev \
        libbz2-dev \
        libgmp3-dev \
        libev-dev \
        libsnappy-dev \
        libxen-dev \
        help2man \
        pkg-config \
        time \
        aspcud \
        wget \
        rsync \
        darcs \
        git \
        unzip \
        protobuf-compiler \
        libgcrypt20-dev \
        libjerasure-dev \
        yasm \
        automake \
        python-dev \
        python-pip \
        debhelper \
        psmisc \
        strace \
        curl \
        g++ \
        libgflags-dev \
        sudo \
        libtool \
        libboost-all-dev \
        fuse

RUN useradd jenkins -u 1001 -g root

RUN pip install fabric junit-xml nose

RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh
RUN sh ./opam_installer.sh /usr/local/bin 4.02.3
RUN mkdir /home/opam
ENV opam_env='opam config env --root=/home/opam/OPAM'
RUN opam init --root=/home/opam/OPAM --comp 4.02.3

RUN eval `${opam_env}` && opam install -y \
        ocamlfind \
        ssl \
        camlbz2 \
        snappy \
        sexplib \
        bisect \
        lwt \
        camltc \
        cstruct \
        ctypes \
        ctypes-foreign \
        uuidm \
        zarith \
        mirage-no-xen.1 \
        quickcheck \
        nocrypto \
        cmdliner \
        conf-libev \
        depext \
        kinetic-client \
        tiny_json \
        ppx_deriving_yojson \
        core_kernel

RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins
ENV LANG=C
