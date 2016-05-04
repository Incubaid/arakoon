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
        net-tools \
        ncurses-dev

RUN useradd jenkins -u 1001 -g root

RUN pip install fabric junit-xml nose simplejson python-etcd

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
        camltc.0.9.3 \
        quickcheck \
        conf-libev \
        ocplib-endian \
        depext \
        redis.0.2.3 \
        uri.1.9.1 \
        core.113.00.00

RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins
ENV LANG=C
RUN chmod 777 /home/opam/OPAM/4.02.3/lib

# Install etcd:
RUN curl -L  https://github.com/coreos/etcd/releases/download/v2.2.4/etcd-v2.2.4-linux-amd64.tar.gz -o etcd-v2.2.4-linux-amd64.tar.gz
RUN tar xzvf etcd-v2.2.4-linux-amd64.tar.gz
RUN cp ./etcd-v2.2.4-linux-amd64/etcd /usr/bin \
    && cp ./etcd-v2.2.4-linux-amd64/etcdctl /usr/bin

# Install redis:
RUN wget http://download.redis.io/releases/redis-3.0.7.tar.gz
RUN tar xzvf redis-3.0.7.tar.gz
RUN cd redis-3.0.7 && make
RUN cp ./redis-3.0.7/src/redis-server /usr/bin
RUN cp ./redis-3.0.7/src/redis-cli /usr/bin
