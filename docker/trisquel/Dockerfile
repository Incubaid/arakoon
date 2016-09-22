FROM kpengboy/trisquel:latest

RUN useradd jenkins -u 1500 -g root

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
        build-essential m4 apt-utils \
        libssl-dev \
        libev-dev \
        libbz2-dev \
        libsnappy-dev \
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
        sudo \
        iptables \
        net-tools \
        ncurses-dev

RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh

env ocaml_version=4.03.0
RUN sh ./opam_installer.sh /usr/local/bin ${ocaml_version}

ENV opam_root=/home/jenkins/OPAM
ENV opam_env="opam config env --root=${opam_root}"
RUN opam init --root=${opam_root} --comp ${ocaml_version}

RUN eval `${opam_env}` && \
    opam update && \
    opam install -y \
        ocamlfind \
        ssl.0.5.2 \
        camlbz2 \
        snappy \
        bisect \
        lwt.2.5.2 \
        camltc.0.9.3 \
        ocplib-endian.1.0 \
        quickcheck.1.0.2 \
        conf-libev \
        core.113.33.02+4.03 \
        redis.0.3.3 \
        uri.1.9.2


RUN pip install fabric junit-xml nose simplejson python-etcd

RUN mkdir /home/tests
RUN chmod 777 /home/tests

# Install etcd:
ENV etcd_version=2.3.5
RUN curl \
      -L https://github.com/coreos/etcd/releases/download/v${etcd_version}/etcd-v${etcd_version}-linux-amd64.tar.gz \
      -o etcd-v${etcd_version}-linux-amd64.tar.gz \
    && tar xzvf etcd-v${etcd_version}-linux-amd64.tar.gz \
    && cp ./etcd-v${etcd_version}-linux-amd64/etcd /usr/bin \
    && cp ./etcd-v${etcd_version}-linux-amd64/etcdctl /usr/bin \
    && rm -rf etcd-*

# Install redis:
RUN wget http://download.redis.io/releases/redis-3.0.7.tar.gz
RUN tar xzvf redis-3.0.7.tar.gz
RUN cd redis-3.0.7 && make
RUN cp ./redis-3.0.7/src/redis-server /usr/bin
RUN cp ./redis-3.0.7/src/redis-cli /usr/bin


RUN chmod ugoa+rxw -R ${opam_root}
RUN su - -c "echo 'eval `${opam_env}`' >> /home/jenkins/.profile"
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins


ENTRYPOINT ["/home/jenkins/arakoon/docker/docker-entrypoint.sh"]
