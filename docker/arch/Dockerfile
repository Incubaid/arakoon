FROM pritunl/archlinux:2016-04-16

RUN useradd jenkins -u 1500 -g root

RUN pacman -Sy --noconfirm gcc clang sudo autoconf base-devel \
                           wget git unzip python2 \
                           libev snappy help2man \
                           python-pip python2

## extra packages not available in the default repo
## makepkg refuses running as root, so switch to jenkins first

RUN echo 'jenkins ALL=NOPASSWD: ALL' >/etc/sudoers.d/jenkins
RUN mkdir /home/jenkins
RUN chown jenkins /home/jenkins

WORKDIR /home/jenkins
USER jenkins

RUN git clone https://aur.archlinux.org/clasp.git && \
    cd clasp && \
    makepkg -sri --noconfirm && \
    cd .. && \
    rm -rf clasp

RUN git clone https://aur.archlinux.org/gringo.git && \
    cd gringo && \
    makepkg -sri --noconfirm && \
    cd .. && \
    rm -rf gringo

RUN git clone https://aur.archlinux.org/aspcud.git && \
    cd aspcud && \
    makepkg -sri --noconfirm && \
    cd .. && \
    rm -rf aspcud

RUN git clone https://aur.archlinux.org/opam.git && \
    cd opam && \
    makepkg -sri --noconfirm && \
    cd .. && \
    rm -rf opam

ENV ocaml_version=4.03.0
ENV opam_root=/home/jenkins/OPAM
ENV opam_env="opam config env --root=${opam_root}"
RUN opam init --root ${opam_root} --comp ${ocaml_version}

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

# Install etcd:
ENV etcd_version=2.3.5
RUN curl \
      -L https://github.com/coreos/etcd/releases/download/v${etcd_version}/etcd-v${etcd_version}-linux-amd64.tar.gz \
      -o etcd-v${etcd_version}-linux-amd64.tar.gz \
    && tar xzvf etcd-v${etcd_version}-linux-amd64.tar.gz \
    && sudo cp ./etcd-v${etcd_version}-linux-amd64/etcd /usr/bin \
    && sudo cp ./etcd-v${etcd_version}-linux-amd64/etcdctl /usr/bin \
    && rm -rf etcd-*

# Install redis:
RUN wget http://download.redis.io/releases/redis-3.0.7.tar.gz
RUN tar xzvf redis-3.0.7.tar.gz
RUN cd redis-3.0.7 && make
RUN sudo cp ./redis-3.0.7/src/redis-server /usr/bin
RUN sudo cp ./redis-3.0.7/src/redis-cli /usr/bin

RUN chmod ugoa+rxw -R ${opam_root}
RUN eval ${opam_env} >> /home/jenkins/.profile

USER root

ENTRYPOINT ["/home/jenkins/arakoon/docker/docker-entrypoint.sh"]
