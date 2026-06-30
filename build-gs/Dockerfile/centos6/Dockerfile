FROM centos:6
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
     yum install -y devtoolset-8-toolchain && \
     scl enable devtoolset-8 bash

RUN wget https://download-ib01.fedoraproject.org/pub/epel/epel-release-latest-6.noarch.rpm

RUN rpm -ivh epel-release-latest-6.noarch.rpm

RUN yum --enablerepo=epel-testing install -y cmake3

RUN yum -y install pam-devel sclo-git212

RUN mv /usr/bin/git /usr/bin/git.bk && ln -s /opt/rh/sclo-git212/root/usr/bin/git /usr/bin/git

RUN yum install -y devtoolset-8-libasan-devel

RUN yum install -y perl perl-Test-Simple perl-Time-HiRes perl-Env perl-Data-Dumper perl-JSON MySQL-python perl-Digest perl-Digest-MD5 perl-Digest-Perl-MD5

RUN yum install -y gperf jemalloc zstd zstd-devel time bison libeatmydata jemalloc-devel pkg-config patchelf ccache

RUN yum install -y devtoolset-8-gcc-c++ devtoolset-8-binutils devtoolset-8-gcc devtoolset-8-gcc-c++ \
    ccache devtoolset-8-libasan-devel devtoolset-8-libubsan-devel devtoolset-8-valgrind devtoolset-8-valgrind-devel libtool libzstd-devel \
    re2-devel redhat-lsb-core lz4-static

RUN ln -fs /usr/bin/cmake3 /usr/bin/cmake

RUN yum install -y libcurl-devel

RUN wget http://gitlab.greatopensource.com/centos6/binary/raw/master/patchelf -O /usr/bin/patchelf && chmod 777 /usr/bin/patchelf

CMD ["tail", "-f", "/dev/null"]
