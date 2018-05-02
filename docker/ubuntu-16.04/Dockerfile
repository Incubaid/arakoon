FROM ubuntu:16.04

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
        ncurses-dev \
        tzdata

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


# most recent one (28/11/2017) is not compatible with our setup
RUN wget https://raw.githubusercontent.com/ocaml/opam/afa822ec24c0f7afa278179cc59796a384129ce1/shell/opam_installer.sh

env ocaml_version=4.06.0+default-unsafe-string
RUN sh ./opam_installer.sh /usr/local/bin ${ocaml_version}

ENV opam_root=/home/jenkins/OPAM
ENV opam_env="opam config env --root=${opam_root}"
RUN opam init --root=${opam_root} --comp ${ocaml_version}

ADD opam.switch opam.switch
ADD opam-repository ovs-opam-repository
RUN chmod ugoa+rxw -R ovs-opam-repository

RUN eval `${opam_env}` && \
    opam repo add ovs ovs-opam-repository && \
    opam update -v && \
    (eval `${opam_env}` && opam switch import opam.switch -y --strict) 

RUN eval `${opam_env}` && opam list && \
    opam switch export opam.switch.out && \
    diff opam.switch opam.switch.out

RUN chmod ugoa+rxw -R ${opam_root}
RUN su - -c "echo 'eval `${opam_env}`' >> /home/jenkins/.profile"
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins


ENTRYPOINT ["/bin/bash", "-c", "set -e && /home/jenkins/arakoon/docker/docker-entrypoint.sh $@"]
