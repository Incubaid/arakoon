FROM ubuntu:14.04.3

RUN useradd jenkins -u 1500 -g root

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
        curl \
        rsync \
        darcs \
        git \
        unzip \
        automake \
        python-dev \
        python-pip \
        debhelper \
        psmisc \
        sudo \
        libtool \
        iptables \
        net-tools \
        ncurses-dev


RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh

env ocaml_version=4.04.2
RUN sh ./opam_installer.sh /usr/local/bin ${ocaml_version}

ENV opam_root=/home/jenkins/OPAM
ENV opam_env="opam config env --root=${opam_root}"
RUN opam init --root=${opam_root} --comp ${ocaml_version}

RUN eval `${opam_env}` && \
    opam update -v && \
    opam install -y \
        oasis.0.4.10 \
        ocamlfind \
        ssl.0.5.3 \
        camlbz2 \
        snappy \
        bisect \
        lwt_ssl \
        lwt.3.0.0 \
        camltc.0.9.4 \
        ocplib-endian.1.0 \
        quickcheck.1.0.2 \
        conf-libev \
        ppx_deriving.4.1 \
        base.v0.9.3 \
        core.v0.9.1 \
        redis.0.3.3 \
        uri.1.9.4

#
RUN wget https://www.python.org/ftp/python/2.7.12/Python-2.7.12.tar.xz
RUN tar -Jxvf ./Python-2.7.12.tar.xz
RUN cd /Python-2.7.12 && ./configure && make && sudo make install
RUN curl https://bootstrap.pypa.io/get-pip.py | python
RUN pip install fabric junit-xml nose simplejson python-etcd

RUN mkdir /home/tests
RUN chmod 777 /home/tests

# Install etcd:
ENV etcd_version=2.3.5
RUN curl -L  https://github.com/coreos/etcd/releases/download/v${etcd_version}/etcd-v${etcd_version}-linux-amd64.tar.gz -o etcd-v{etcd_version}-linux-amd64.tar.gz
RUN tar xzvf etcd-v{etcd_version}-linux-amd64.tar.gz
RUN cp ./etcd-v${etcd_version}-linux-amd64/etcd /usr/bin \
    && cp ./etcd-v${etcd_version}-linux-amd64/etcdctl /usr/bin

# Install redis:
RUN wget http://download.redis.io/releases/redis-3.0.7.tar.gz
RUN tar xzvf redis-3.0.7.tar.gz
RUN cd redis-3.0.7 && make
RUN cp ./redis-3.0.7/src/redis-server /usr/bin
RUN cp ./redis-3.0.7/src/redis-cli /usr/bin


RUN chmod ugoa+rxw -R ${opam_root}
RUN su - -c "echo 'eval `${opam_env}`' >> /home/jenkins/.profile"
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins



ENTRYPOINT ["/bin/bash", "-c", "set -e && /home/jenkins/arakoon/docker/docker-entrypoint.sh $@"]
