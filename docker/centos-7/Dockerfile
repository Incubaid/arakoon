FROM centos:7

RUN useradd jenkins -u 1500 -g root

RUN rpm --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
RUN yum -y update && yum -y install \
        wget curl \
        make m4 gcc patch unzip \
        git rsync mercurial \
        gcc-c++ \
        bzip2-devel libffi-devel snappy-devel libev-devel \
        python-devel

# TODO: What about darcs ?

RUN rpm -iUvh http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-8.noarch.rpm

RUN yum -y update && yum -y install \
         python-pip \
         openssl-devel gmp-devel which \
         zlib-devel ncurses-devel


RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh

env ocaml_version=4.03.0
RUN sh ./opam_installer.sh /usr/local/bin ${ocaml_version}

ENV opam_root=/home/jenkins/OPAM
ENV opam_env="opam config env --root=${opam_root}"
RUN opam init --root=${opam_root} --comp ${ocaml_version}

RUN git clone git://github.com/janestreet/opam-repository.git /janestreet-opam-repo && \
    cd /janestreet-opam-repo && \
    git checkout b98fd1964856f0c0b022a42ec4e6fc6c7bad2e81
RUN eval `${opam_env}` && \
    opam repo --root=${opam_root} -k local add janestreet /janestreet-opam-repo

# protobuf
RUN yum -y install bzip2
RUN wget https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.bz2 -O protobuf-2.6.1.tar.bz2 \
    && tar -jxvf protobuf-2.6.1.tar.bz2 \
    && cd protobuf-2.6.1 \
    && ./configure && make && make install

#RUN rpm -ivh http://cbs.centos.org/kojifiles/packages/protobuf/2.5.0/10.el7.centos/src/protobuf-2.5.0-10.el7.centos.src.rpm

RUN pip install fabric junit-xml

RUN wget http://cudf-solvers.irill.org/cudf_remote_proxy
RUN chmod u+x cudf_remote_proxy
RUN mv cudf_remote_proxy /usr/local/bin/

ENV OPAMEXTERNALSOLVER="cudf_remote_proxy %{input}% %{output}% %{criteria}%"

RUN eval `${opam_env}` && \
    opam update && \
    opam install -y \
        oasis.0.4.6 \
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
        core.114.05+21 \
        redis.0.3.3 \
        uri.1.9.2


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


RUN yum -y update && yum -y install \
        autoconf help2man perl-Thread-Queue \
        sudo iproute

#
# Disable "ssh hostname sudo <cmd>", because it will show the password in clear.
#         You have to run "ssh -t hostname sudo <cmd>".
#
# Defaults    requiretty          # is line 56
RUN awk 'NR == 56 {next} {print}' /etc/sudoers >/tmp/__sudoers && mv /tmp/__sudoers /etc/sudoers

RUN yum -y install rpm-build

RUN chmod ugoa+rxw -R ${opam_root}
RUN su - -c "echo 'eval `${opam_env}`' >> /home/jenkins/.bash_profile"
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins


ENTRYPOINT ["/home/jenkins/arakoon/docker/docker-entrypoint.sh"]
