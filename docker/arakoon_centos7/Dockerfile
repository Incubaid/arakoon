FROM centos:7

RUN rpm --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
RUN yum -y install wget
RUN yum -y install curl
RUN yum -y install make m4 gcc patch unzip
RUN yum -y install git rsync mercurial
RUN yum -y install gcc-c++
RUN yum -y install bzip2-devel libffi-devel snappy-devel libev-devel
RUN yum -y install python-devel

# TODO: What about darcs ?

RUN useradd jenkins -u 1001 -g root

RUN rpm -iUvh http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-5.noarch.rpm
RUN yum -y update
RUN yum -y install python-pip
RUN yum -y install openssl-devel gmp-devel which
RUN yum -y install zlib-devel ncurses-devel

RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh
RUN sh ./opam_installer.sh /usr/local/bin 4.02.3
RUN mkdir /home/opam
ENV opam_env='opam config env --root=/home/opam/OPAM'
RUN opam init --root=/home/opam/OPAM --comp 4.02.3


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
        redis.0.3.1 \
        uri.1.9.1 \
        core.113.00.00


RUN yum -y install autoconf help2man perl-Thread-Queue

RUN yum -y install sudo iproute
#
# Disable "ssh hostname sudo <cmd>", because it will show the password in clear.
#         You have to run "ssh -t hostname sudo <cmd>".
#
# Defaults    requiretty          # is line 56
RUN awk 'NR == 56 {next} {print}' /etc/sudoers >/tmp/__sudoers && mv /tmp/__sudoers /etc/sudoers

RUN yum -y install rpm-build

CMD eval `${opam_env}` && opam list
