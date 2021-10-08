FROM centos:7
RUN yum install  -y \
    bison-devel \
    bzip2 \
    centos-release-scl \
    cmake \
    expat-devel  \
    gcc \
    gflags-devel \
    git \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel \
    libaio-devel \
    libcurl-dev \
    libevent-devel \
    libgnomecanvas-devel \
    libicu-devel \
    lz4-devel \
    mysql-devel \
    ncurses-devel \
    ncurses-libs \
    numactl-devel \
    openssl-devel \
    openssl-devel \
    package \
    pam \
    perl-ExtUtils-MakeMaker \
    python-devel \
    readline-devel \
    redhat-lsb-core \
    rpm* \
    scl-utils-build \
    unzip \
    vim \
    wget \
    openldap-devel \
    zlib-devel
RUN  yum groupinstall "Development Tools" -y && \
     yum install -y devtoolset-10-toolchain && \
     scl enable devtoolset-10 bash

RUN wget https://download-ib01.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

RUN rpm -ivh epel-release-latest-7.noarch.rpm

RUN yum --enablerepo=epel-testing install -y cmake3

RUN yum -y install pam-devel sclo-git212

RUN mv /usr/bin/git /usr/bin/git.bk && ln -s /opt/rh/sclo-git212/root/bin/git /usr/bin/git

RUN yum install -y devtoolset-10-libasan-devel

RUN yum install -y perl perl-Test-Simple perl-Time-HiRes perl-Env perl-Data-Dumper perl-JSON MySQL-python perl-Digest perl-Digest-MD5 perl-Digest-Perl-MD5

RUN yum install -y gperf jemalloc time bison libeatmydata jemalloc-devel pkg-config patchelf ccache

RUN yum install -y devtoolset-10-gcc-c++ devtoolset-10-binutils devtoolset-10-gcc devtoolset-10-gcc-c++ \
    ccache devtoolset-10-libasan-devel devtoolset-10-libubsan-devel devtoolset-10-valgrind devtoolset-10-valgrind-devel libtool \
    re2-devel redhat-lsb-core lz4-static

RUN ln -fs /usr/bin/cmake3 /usr/bin/cmake

RUN yum install -y libcurl-devel

CMD ["tail", "-f", "/dev/null"]
