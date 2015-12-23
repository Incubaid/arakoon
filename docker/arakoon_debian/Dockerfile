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
        iptables \
        net-tools

RUN useradd jenkins -u 1001 -g root

RUN pip install fabric junit-xml nose

RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh
RUN sh ./opam_installer.sh /usr/local/bin 4.02.3

RUN mkdir /home/tests
RUN chmod 777 /home/tests
ENV TEST_HOME='/home/tests'
ENV PATH=${PATH}:/home/tests/apps/arakoon/bin

RUN mkdir /home/opam
ENV opam_env='opam config env --root=/home/opam/OPAM'
RUN opam init --root=/home/opam/OPAM --comp 4.02.3

RUN eval `${opam_env}` && \
    opam update && \
    opam install -y \
        ocamlfind \
        ssl.0.5.2 \
        camlbz2 \
        snappy \
        bisect \
        lwt.2.5.1 \
        camltc \
        quickcheck \
        conf-libev \
        ocplib-endian \
        depext

RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins
ENV LANG=C
RUN chmod 777 /home/opam/OPAM/4.02.3/lib
