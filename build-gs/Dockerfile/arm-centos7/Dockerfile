FROM docker.io/arm64v8/centos:centos7
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
    zlib-devel \
    centos-release-scl-rh
RUN yum groupinstall "Development Tools" -y
RUN yum-config-manager --enable centos-sclo-rh-testing
RUN yum install -y devtoolset-10 && scl enable devtoolset-10 bash
RUN yum install -y epel-release
RUN yum install -y cmake3 tar

RUN yum -y install pam-devel libcurl-devel

# update git
RUN wget https://github.com/git/git/archive/v2.27.0.tar.gz
RUN tar -xzvf v2.27.0.tar.gz && cd git-2.27.0 && make prefix=/opt/git/ all && make prefix=/opt/git/ install
RUN mv /usr/bin/git /usr/bin/git.bk && ln -s /opt/git/bin/git /usr/bin/git

# update patchelf 0.11
RUN git clone https://github.com/NixOS/patchelf && cd patchelf && ./bootstrap.sh && ./configure && make && make install

RUN yum install -y devtoolset-10-libasan-devel

RUN yum install -y perl perl-Test-Simple perl-Time-HiRes perl-Env perl-Data-Dumper perl-JSON MySQL-python perl-Digest perl-Digest-MD5 perl-Digest-Perl-MD5

RUN yum install -y gperf jemalloc time bison libeatmydata jemalloc-devel pkg-config ccache

RUN yum install -y devtoolset-10-gcc-c++ devtoolset-10-binutils devtoolset-10-gcc devtoolset-10-gcc-c++ \
    ccache devtoolset-10-libasan-devel devtoolset-10-libubsan-devel devtoolset-10-valgrind devtoolset-10-valgrind-devel libtool \
    re2-devel redhat-lsb-core lz4-static

RUN ln -fs /usr/bin/cmake3 /usr/bin/cmake

CMD ["tail", "-f", "/dev/null"]
