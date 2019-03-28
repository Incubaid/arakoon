FROM ubuntu:16.04

ENV release_name=xenial
ENV etcd_version=2.3.5
ENV redis_version=3.0.7
ENV ocaml_version=4.07.1+default-unsafe-string


# force our apt to use look at mirrors (by prepending a mirrors line)
# RUN sed 's@archive.ubuntu.com@ubuntu.mirror.atratoip.net@' -i /etc/apt/sources.list
RUN sed -i "1s;^;deb mirror://mirrors.ubuntu.com/mirrors.txt ${release_name}-updates main restricted universe multiverse\n;" /etc/apt/sources.list
RUN sed -i "1s;^;deb mirror://mirrors.ubuntu.com/mirrors.txt ${release_name}         main restricted universe multiverse\n;" /etc/apt/sources.list
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
        tzdata \
        librdmacm-dev \
        protobuf-compiler \
        socat

ARG HOST_UID
RUN useradd jenkins -u $HOST_UID -g root --create-home
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins

RUN pip install --upgrade pip
RUN pip --disable-pip-version-check install fabric junit-xml nose simplejson python-etcd

# Install etcd:

RUN wget -q \
    https://github.com/coreos/etcd/releases/download/v${etcd_version}/etcd-v${etcd_version}-linux-amd64.tar.gz -O - \
    | tar zxf - \
    && cp etcd-v${etcd_version}-linux-amd64/etcd /usr/bin \
    && cp etcd-v${etcd_version}-linux-amd64/etcdctl /usr/bin \
    && rm -rf etcd-*

# Install redis:

RUN wget -q \
    http://download.redis.io/releases/redis-${redis_version}.tar.gz -O - \
    | tar zxf - \
    && make -j$(nproc 2>/dev/null || echo 1) -C redis-${redis_version} \
    && cp redis-${redis_version}/src/redis-server /usr/bin \
    && cp redis-${redis_version}/src/redis-cli /usr/bin \
    && rm -rf redis-${redis_version}

RUN wget https://github.com/ocaml/opam/releases/download/2.0.3/opam-2.0.3-x86_64-linux \
    && mv opam-2.0.3-x86_64-linux /usr/bin/opam \
    && chmod a+x /usr/bin/opam

USER jenkins


ENV OPAMROOT=/home/jenkins/OPAM

# The special variable NO_PROXY contains host and port for accessing
# the hosts SSH_AUTH_SOCK
ENV SSH_AUTH_SOCK /tmp/auth.sock
RUN mkdir /home/jenkins/.ssh
RUN echo "\nHost *\n    StrictHostKeyChecking=no" >> /home/jenkins/.ssh/config

env opam_env="opam config env --root=${OPAMROOT}"

RUN opam init --root ${OPAMROOT} --compiler=${ocaml_version} --disable-sandboxing
ADD opam.switch opam.switch
ADD opam-repository ovs-opam-repository

RUN socat UNIX-LISTEN:${SSH_AUTH_SOCK},unlink-early,mode=777,fork TCP:${NO_PROXY} & \
    eval `${opam_env}` && \
    opam repo add ovs ovs-opam-repository && \
    opam update -v

RUN socat UNIX-LISTEN:${SSH_AUTH_SOCK},unlink-early,mode=777,fork TCP:${NO_PROXY} & \
    eval `${opam_env}` && export OPAMROOT=${OPAMROOT} && \
    opam switch import opam.switch -y --strict

RUN eval ${opam_env} && export OPAMROOT=${OPAMROOT} && \
    opam list && \
    opam switch export /home/jenkins/opam.switch.out && ls && \
    cat /home/jenkins/opam.switch.out

RUN opam list && bash -c 'diff opam.switch <(opam switch export -)'

ENTRYPOINT ["/home/jenkins/arakoon/docker/docker-entrypoint.sh"]
