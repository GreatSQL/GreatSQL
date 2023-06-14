%undefine _missing_build_ids_terminate_build
%global mysql_vendor Oracle and/or its affiliates
%global greatsql_vendor GreatDB Software Co., Ltd.
%global mysqldatadir /var/lib/mysql

%global mysql_version 8.0.32
%global greatsql_version 24
%global revision 3714067bc8c
%global tokudb_backup_version %{mysql_version}-%{greatsql_version}
%global rpm_release 1

%global release %{greatsql_version}.%{rpm_release}%{?dist}

# By default, a build will be done using the system SSL library
%{?with_ssl: %global ssl_option -DWITH_SSL=%{with_ssl}}
%{!?with_ssl: %global ssl_option -DWITH_SSL=system}

# By default a build will be done including the TokuDB
%{!?with_tokudb: %global tokudb 0}

# By default a build will be done including the RocksDB
%{!?with_rocksdb: %global rocksdb 0}

# Pass path to mecab lib
%{?with_mecab: %global mecab_option -DWITH_MECAB=%{with_mecab}}
%{?with_mecab: %global mecab 1}

# Regression tests may take a long time, override the default to skip them
%{!?runselftest:%global runselftest 0}

%{!?with_systemd:                %global systemd 0}
%global systemd 1
%{!?with_debuginfo:              %global nodebuginfo 0}
%{!?product_suffix:              %global product_suffix -80}
%{!?feature_set:                 %global feature_set community}
%{!?compilation_comment_release: %global compilation_comment_release GreatSQL (GPL), Release %{greatsql_version}, Revision %{revision}}
%{!?compilation_comment_debug:   %global compilation_comment_debug GreatSQL - Debug (GPL), Release %{greatsql_version}, Revision %{revision}}
%{!?src_base:                    %global src_base greatsql}

# Setup cmake flags for TokuDB
%if 0%{?tokudb}
  %global TOKUDB_FLAGS -DWITH_VALGRIND=OFF -DUSE_VALGRIND=OFF -DDEBUG_EXTNAME=OFF -DBUILD_TESTING=OFF -DUSE_GTAGS=OFF -DUSE_CTAGS=OFF -DUSE_ETAGS=OFF -DUSE_CSCOPE=OFF -DTOKUDB_BACKUP_PLUGIN_VERSION=%{tokudb_backup_version}
  %global TOKUDB_DEBUG_ON -DTOKU_DEBUG_PARANOID=ON
  %global TOKUDB_DEBUG_OFF -DTOKU_DEBUG_PARANOID=OFF
%else
  %global TOKUDB_FLAGS -DWITHOUT_TOKUDB=1
  %global TOKUDB_DEBUG_ON %{nil}
  %global TOKUDB_DEBUG_OFF %{nil}
%endif

# Setup cmake flags for RocksDB
%if 0%{?rocksdb}
  %global ROCKSDB_FLAGS -DWITH_ROCKSDB=0
%else
  %global ROCKSDB_FLAGS -DWITH_ROCKSDB=0
%endif

%global shared_lib_pri_name mysqlclient
%global shared_lib_sec_name perconaserverclient

# multiarch
%global multiarchs            ppc %{power64} %{ix86} x86_64 %{sparc} %{arm} aarch64

%global src_dir               %{src_base}-%{mysql_version}-%{greatsql_version}

# We build debuginfo package so this is not used
%if 0%{?nodebuginfo}
%global _enable_debug_package 0
%global debug_package         %{nil}
%global __os_install_post     /usr/lib/rpm/brp-compress %{nil}
%endif

%global license_files_server  %{src_dir}/README.md
%global license_type          GPLv2

Name:           greatsql
Summary:        GreatSQL: a high performance, highly reliable, easy to use, and high security database
Group:          Applications/Databases
Version:        %{mysql_version}
Release:        %{release}
License:        Copyright (c) 2000, 2018, %{mysql_vendor}. All rights reserved. Under %{?license_type} license as shown in the Description field..
SOURCE0:        greatsql-8.0.32-24.tar.xz
URL:            https://greatsql.cn
SOURCE5:        mysql_config.sh
SOURCE10:       boost_1_77_0.tar.xz
SOURCE90:       filter-provides.sh
SOURCE91:       filter-requires.sh
SOURCE11:       mysqld.cnf
Patch0:         mysql-5.7-sharedlib-rename.patch
BuildRequires:  cmake >= 2.8.2
BuildRequires:  make
BuildRequires:  gcc
BuildRequires:  gcc-c++
BuildRequires:  perl
#BuildRequires:	perl(Time::HiRes)}
#BuildRequires:	perl(Env)}
BuildRequires:  perl(Carp)
BuildRequires:  perl(Config)
BuildRequires:  perl(Cwd)
BuildRequires:  perl(Data::Dumper)
BuildRequires:  perl(English)
BuildRequires:  perl(Errno)
BuildRequires:  perl(Exporter)
BuildRequires:  perl(Fcntl)
BuildRequires:  perl(File::Basename)
BuildRequires:  perl(File::Copy)
BuildRequires:  perl(File::Find)
BuildRequires:  perl(File::Path)
BuildRequires:  perl(File::Spec)
BuildRequires:  perl(File::Spec::Functions)
BuildRequires:  perl(File::Temp)
BuildRequires:  perl(Getopt::Long)
BuildRequires:  perl(IO::File)
BuildRequires:  perl(IO::Handle)
BuildRequires:  perl(IO::Pipe)
BuildRequires:  perl(IO::Select)
BuildRequires:  perl(IO::Socket)
BuildRequires:  perl(IO::Socket::INET)
BuildRequires:  perl(JSON)
BuildRequires:  perl(Memoize)
BuildRequires:  perl(POSIX)
BuildRequires:  perl(Sys::Hostname)
BuildRequires:  perl(Time::HiRes)
BuildRequires:  perl(Time::localtime)
BuildRequires:  time
BuildRequires:  libaio-devel
BuildRequires:  ncurses-devel
BuildRequires:  pam-devel
BuildRequires:  readline-devel
BuildRequires:  numactl-devel
#BuildRequires:  compat-openssl11-devel
BuildRequires:  openssl
BuildRequires:  openssl-devel
BuildRequires:  zlib-devel
BuildRequires:  bison
BuildRequires:  openldap-devel
BuildRequires:  libcurl-devel
%if 0%{?systemd}
BuildRequires:  systemd
BuildRequires:  pkgconfig(systemd)
%endif
BuildRequires:  cyrus-sasl-devel
BuildRequires:  openldap-devel

BuildRequires:  cmake >= 3.6.1
BuildRequires:  gcc
BuildRequires:  gcc-c++
BuildRequires:  libtirpc-devel
BuildRequires:  rpcgen
BuildRequires:  m4
BuildRoot:      %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

# For rpm => 4.9 only: https://fedoraproject.org/wiki/Packaging:AutoProvidesAndRequiresFiltering
%global __requires_exclude ^perl\\((GD|hostnames|lib::mtr|lib::v1|mtr_|My::)
%global __provides_exclude_from ^(/usr/share/(mysql|mysql-test)/.*|%{_libdir}/mysql/plugin/.*\\.so|/usr/include/mysql/.*|/usr/share/man/man.*/mysql.*|/etc/my.cnf|/usr/bin/mysql.*|/usr/sbin/mysqld.*|*libprotobuf*|*libmysqlclient.so*|*libmysqlharness*|*libmysqlrouter*|*mysqlclient*|*libdaemon*|*libfnv*|*libmemcached*|*libmurmur*|*libtest*)$

%global _privatelibs lib(protobuf|mysqlclient|mysqlharness|mysqlrouter|mysqlclient|daemon|fnv|memcached|murmur|test)*\\.so*
%global __provides_exclude %{_privatelibs}
%global __requires_exclude %{_privatelibs}

%description
GreatSQL focuses on improving the reliability and performance of MGR, supports InnoDB parallel query and other features, and is a domestic MySQL version suitable for financial applications. It can be used as an optional replacement of MySQL or Percona Server. It is completely free and compatible with MySQL or Percona server.

For a description of GreatSQL see https://greatsql.cn

%package -n greatsql-server
Summary:        GreatSQL: a high performance, highly reliable, easy to use, and high security database that can be used to replace MySQL or Percona Server.
Group:          Applications/Databases
Requires:       coreutils
Requires:       bash /bin/sh
Requires:       grep
Requires:       procps
Requires:       shadow-utils
Requires:       net-tools
Requires(pre):  greatsql-shared
Requires:       greatsql-client
Requires:       greatsql-icu-data-files
#Requires:       compat-openssl11-devel
Requires:       openssl
Conflicts:      Percona-SQL-server-50 Percona-Server-server-51 Percona-Server-server-55 Percona-Server-server-56 Percona-Server-server-57

%if 0%{?systemd}
Requires(post):   systemd
Requires(preun):  systemd
Requires(postun): systemd
%else
Requires(post):   /sbin/chkconfig
Requires(preun):  /sbin/chkconfig
Requires(preun):  /sbin/service
%endif

Obsoletes:      mariadb-connector-c-config

%description -n greatsql-server
GreatSQL: a high performance, highly reliable, easy to use, and high security database that can be used to replace MySQL or Percona Server.

For a description of GreatSQL see https://greatsql.cn

%package -n greatsql-client
Summary:        GreatSQL - Client
Group:          Applications/Databases
Requires:       greatsql-shared
Conflicts:      Percona-SQL-client-50 Percona-Server-client-51 Percona-Server-client-55 Percona-Server-client-56 Percona-Server-client-57

%description -n greatsql-client
This package contains the standard GreatSQL client and administration tools.

For a description of GreatSQL see https://greatsql.cn

%package -n greatsql-test
Summary:        Test suite for the GreatSQL
Group:          Applications/Databases
Requires:       perl(Carp)
Requires:       perl(Config)
Requires:       perl(Cwd)
Requires:       perl(Data::Dumper)
Requires:       perl(English)
Requires:       perl(Errno)
Requires:       perl(Exporter)
Requires:       perl(Fcntl)
Requires:       perl(File::Basename)
Requires:       perl(File::Copy)
Requires:       perl(File::Find)
Requires:       perl(File::Path)
Requires:       perl(File::Spec)
Requires:       perl(File::Spec::Functions)
Requires:       perl(File::Temp)
Requires:       perl(Getopt::Long)
Requires:       perl(IO::File)
Requires:       perl(IO::Handle)
Requires:       perl(IO::Pipe)
Requires:       perl(IO::Select)
Requires:       perl(IO::Socket)
Requires:       perl(IO::Socket::INET)
Requires:       perl(JSON)
Requires:       perl(Memoize)
Requires:       perl(POSIX)
Requires:       perl(Sys::Hostname)
Requires:       perl(Time::HiRes)
Requires:       perl(Time::localtime)
Requires(pre):  greatsql-shared greatsql-client greatsql-server
Obsoletes:      MySQL-test < %{version}-%{release}
Obsoletes:      mysql-test < %{version}-%{release}
Obsoletes:      mariadb-test
Conflicts:      Percona-SQL-test-50 Percona-Server-test-51 Percona-Server-test-55 Percona-Server-test-56 Percona-Server-test-57

%description -n greatsql-test
This package contains the GreatSQL regression test suite.

For a description of GreatSQL see https://greatsql.cn

%package -n greatsql-devel
Summary:        GreatSQL - Development header files and libraries
Group:          Applications/Databases
Conflicts:      Percona-SQL-devel-50 Percona-Server-devel-51 Percona-Server-devel-55 Percona-Server-devel-56 Percona-Server-devel-57
Obsoletes:      mariadb-connector-c-devel
Obsoletes:      mariadb-devel

%description -n greatsql-devel
This package contains the development header files and libraries necessary
to develop GreatSQL client applications.

For a description of GreatSQL see https://greatsql.cn

%package -n greatsql-shared
Summary:        GreatSQL - Shared libraries
Group:          Applications/Databases
Obsoletes:      mysql-libs < %{version}-%{release}

%description -n greatsql-shared
This package contains the shared libraries (*.so*) which certain languages
and applications need to dynamically load and use GreatSQL.

For a description of GreatSQL see https://greatsql.cn

%if 0%{?compatlib}
%package -n greatsql-shared-compat
Summary:        Shared compat libraries for GreatSQL %{compatver}-%{percona_compatver} database client applications
Group:          Applications/Databases

Obsoletes:      mysql-libs

Conflicts:      Percona-Server-shared-51
Conflicts:      Percona-Server-shared-55
Conflicts:      Percona-Server-shared-55
Conflicts:      Percona-Server-shared-56
Conflicts:      Percona-Server-shared-57

%description -n greatsql-shared-compat
This package contains the shared compat libraries for GreatSQL %{compatver}-%{percona_compatver} client
applications.
%endif

%if 0%{?tokudb}
%package -n greatsql-tokudb
Summary:        GreatSQL - TokuDB package
Group:          Applications/Databases
Requires:       greatsql-server = %{version}-%{release}
Requires:       greatsql-shared = %{version}-%{release}
Requires:       greatsql-client = %{version}-%{release}
Requires:       jemalloc >= 3.3.0

%description -n greatsql-tokudb
This package contains the TokuDB plugin for GreatSQL %{version}-%{release}
%endif

%if 0%{?rocksdb}
%package -n greatsql-rocksdb
Summary:        GreatSQL - RocksDB package
Group:          Applications/Databases
Requires:       greatsql-server = %{version}-%{release}
Requires:       greatsql-shared = %{version}-%{release}
Requires:       greatsql-client = %{version}-%{release}

%description -n greatsql-rocksdb
This package contains the RocksDB plugin for GreatSQL %{version}-%{release}

For a description of GreatSQL see https://greatsql.cn
%endif

%package  -n   greatsql-mysql-router
Summary:       GreatSQL MySQL Router
Group:         Applications/Databases
Provides:      greatsql-mysql-router = %{version}-%{release}
Obsoletes:     greatsql-mysql-router < %{version}-%{release}

%description -n greatsql-mysql-router
The GreatSQL MySQL Router software delivers a fast, multi-threaded way of
routing connections from GreatSQL Clients to GreatSQL Servers.

For a description of GreatSQL see https://greatsql.cn

%package   -n   greatsql-mysql-router-devel
Summary:        Development header files and libraries for GreatSQL MySQL Router
Group:          Applications/Databases
Provides:       greatsql-mysql-router-devel = %{version}-%{release}
Obsoletes:      mysql-router-devel

%description -n greatsql-mysql-router-devel
This package contains the development header files and libraries
necessary to develop GreatSQL MySQL Router applications.

For a description of GreatSQL see https://greatsql.cn

%package   -n   greatsql-mysql-config
Summary:        GreatSQL config
Provides:       greatsql-mysql-config = %{version}-%{release}
Conflicts:      mysql-config

%description -n greatsql-mysql-config
This package contains my.cnf for GreatSQL.

For a description of GreatSQL see https://greatsql.cn

%package   -n   greatsql-icu-data-files
Summary:        GreatSQL packaging of ICU data files

%description -n greatsql-icu-data-files
This package contains ICU data files needer by GreatSQL regular expressions.

For a description of GreatSQL see https://greatsql.cn

%prep
%setup -q -T -a 0 -a 10 -c -n %{src_dir}
pushd %{src_dir}
%patch0 -p0
cp %{SOURCE11} scripts

%build
# Fail quickly and obviously if user tries to build as root
%if 0%{?runselftest}
if [ "x$(id -u)" = "x0" ] ; then
   echo "The MySQL regression tests may fail if run as root."
   echo "If you really need to build the RPM as root, use"
   echo "--define='runselftest 0' to skip the regression tests."
   exit 1
fi
%endif

# Build debug versions of mysqld and libmysqld.a
mkdir debug
(
  cd debug
  # Attempt to remove any optimisation flags from the debug build
  optflags=$(echo "%{optflags}" | sed -e 's/-O2 / /' -e 's/-Wp,-D_FORTIFY_SOURCE=2/ -Wno-missing-field-initializers -Wno-error /')
	optflags=$(echo $optflags | sed -e 's/-specs=\/usr\/lib\/rpm\/redhat\/redhat-hardened-cc1 -specs=\/usr\/lib\/rpm\/redhat\/redhat-annobin-cc1/ /')
  cmake ../%{src_dir} \
           -DBUILD_CONFIG=mysql_release \
           -DINSTALL_LAYOUT=RPM \
           -DCMAKE_BUILD_TYPE=Debug \
           -DWITH_BOOST=.. \
           -DCMAKE_C_FLAGS="$optflags -fcommon" \
           -DCMAKE_CXX_FLAGS="$optflags -fcommon" \
%if 0%{?systemd}
           -DWITH_SYSTEMD=1 \
%endif
           -DWITH_INNODB_MEMCACHED=1 \
           -DINSTALL_LIBDIR="%{_lib}/mysql" \
           -DINSTALL_PLUGINDIR="%{_lib}/mysql/plugin" \
           -DMYSQL_UNIX_ADDR="%{mysqldatadir}/mysql.sock" \
           -DINSTALL_MYSQLSHAREDIR=share/greatsql \
           -DINSTALL_SUPPORTFILESDIR=share/greatsql \
           -DFEATURE_SET="%{feature_set}" \
           -DWITH_AUTHENTICATION_LDAP=OFF \
           -DWITH_PAM=1 \
           -DWITH_ROCKSDB=0 \
           -DALLOW_NO_SSE42=ON \
           -DROCKSDB_DISABLE_AVX2=0 \
           -DROCKSDB_DISABLE_MARCH_NATIVE=0 \
           -DGROUP_REPLICATION_WITH_ROCKSDB=OFF \
           -DWITH_TOKUDB=0 \
           -DWITH_INNODB_MEMCACHED=1 \
           -DMYSQL_MAINTAINER_MODE=OFF \
           -DFORCE_INSOURCE_BUILD=1 \
           -DWITH_NUMA=ON \
           -DWITH_LDAP=system \
           -DWITH_SYSTEM_LIBS=ON \
           -DWITH_PROTOBUF=bundled \
           -DWITH_RAPIDJSON=bundled \
           -DWITH_ICU=bundled \
           -DWITH_LZ4=bundled \
           -DWITH_ZLIB=bundled \
           -DWITH_ZSTD=bundled \
           -DWITH_READLINE=system \
           -DWITH_LIBEVENT=bundled \
           -DWITH_KEYRING_VAULT=ON \
           -DWITH_FIDO=bundled \
           %{?ssl_option} \
           %{?mecab_option} \
           -DCOMPILATION_COMMENT="%{compilation_comment_debug}" %{TOKUDB_FLAGS} %{TOKUDB_DEBUG_OFF} %{ROCKSDB_FLAGS}
  echo BEGIN_DEBUG_CONFIG ; egrep '^#define' include/config.h ; echo END_DEBUG_CONFIG
  make %{?_smp_mflags} VERBOSE=1
)
# Build full release
mkdir release
(
  cd release
  cmake ../%{src_dir} \
           -DBUILD_CONFIG=mysql_release \
           -DINSTALL_LAYOUT=RPM \
           -DCMAKE_BUILD_TYPE=RelWithDebInfo \
           -DWITH_BOOST=.. \
           -DCMAKE_C_FLAGS="%{optflags} -fcommon" \
           -DCMAKE_CXX_FLAGS="%{optflags} -fcommon" \
%if 0%{?systemd}
           -DWITH_SYSTEMD=1 \
%endif
           -DWITH_INNODB_MEMCACHED=1 \
           -DINSTALL_LIBDIR="%{_lib}/mysql" \
           -DINSTALL_PLUGINDIR="%{_lib}/mysql/plugin" \
           -DMYSQL_UNIX_ADDR="%{mysqldatadir}/mysql.sock" \
           -DINSTALL_MYSQLSHAREDIR=share/greatsql \
           -DINSTALL_SUPPORTFILESDIR=share/greatsql \
           -DFEATURE_SET="%{feature_set}" \
           -DWITH_AUTHENTICATION_LDAP=OFF \
           -DWITH_PAM=1 \
           -DWITH_ROCKSDB=0 \
           -DROCKSDB_DISABLE_AVX2=0 \
           -DROCKSDB_DISABLE_MARCH_NATIVE=0 \
           -DWITH_TOKUDB=0 \
           -DGROUP_REPLICATION_WITH_ROCKSDB=OFF \
           -DALLOW_NO_SSE42=ON \
           -DWITH_INNODB_MEMCACHED=1 \
           -DMYSQL_MAINTAINER_MODE=OFF \
           -DFORCE_INSOURCE_BUILD=1 \
           -DWITH_NUMA=ON \
           -DWITH_LDAP=system \
           -DWITH_SYSTEM_LIBS=ON \
           -DWITH_LZ4=bundled \
           -DWITH_ZLIB=bundled \
           -DWITH_PROTOBUF=bundled \
           -DWITH_RAPIDJSON=bundled \
           -DWITH_ICU=bundled \
           -DWITH_READLINE=system \
           -DWITH_LIBEVENT=bundled \
           -DWITH_ZSTD=bundled \
           -DWITH_KEYRING_VAULT=ON \
           -DWITH_FIDO=bundled \
           %{?ssl_option} \
           %{?mecab_option} \
           -DCOMPILATION_COMMENT="%{compilation_comment_release}" %{TOKUDB_FLAGS} %{TOKUDB_DEBUG_OFF} %{ROCKSDB_FLAGS}
  echo BEGIN_NORMAL_CONFIG ; egrep '^#define' include/config.h ; echo END_NORMAL_CONFIG
  make %{?_smp_mflags} VERBOSE=1
)

%install
%define _unpackaged_files_terminate_build 0
MBD=$RPM_BUILD_DIR/%{src_dir}

# Ensure that needed directories exists
install -d -m 0751 %{buildroot}/var/lib/mysql
install -d -m 0755 %{buildroot}/var/run/mysqld
install -d -m 0750 %{buildroot}/var/lib/mysql-files
install -d -m 0750 %{buildroot}/var/lib/mysql-keyring

# Router directories
install -d -m 0755 %{buildroot}/var/log/mysqlrouter
install -d -m 0755 %{buildroot}/var/run/mysqlrouter

# Install all binaries
cd $MBD/release
make DESTDIR=%{buildroot} install

# Install logrotate and autostart
#install -D -m 0644 packaging/rpm-common/mysql.logrotate %{buildroot}%{_sysconfdir}/logrotate.d/mysql
#investigate this logrotate
install -D -m 0644 $MBD/release/support-files/mysql-log-rotate %{buildroot}%{_sysconfdir}/logrotate.d/mysql
install -D -m 0644 $MBD/%{src_dir}/build-ps/rpm/mysqld.cnf %{buildroot}%{_sysconfdir}/my.cnf
install -D -p -m 0644 %{_builddir}/greatsql-%{version}-%{greatsql_version}/greatsql-%{version}-%{greatsql_version}/scripts/mysqld.cnf %{buildroot}%{_sysconfdir}/my.cnf
install -d %{buildroot}%{_sysconfdir}/my.cnf.d

#%if 0%{?systemd}
#%else
#%if 0%{?rhel} < 7
#  install -D -m 0755 $MBD/%{src_dir}/build-ps/rpm/mysql.init %{buildroot}%{_sysconfdir}/init.d/mysql
#%endif


# Add libdir to linker
install -d -m 0755 %{buildroot}%{_sysconfdir}/ld.so.conf.d
echo "%{_libdir}/mysql" > %{buildroot}%{_sysconfdir}/ld.so.conf.d/mysql-%{_arch}.conf

# multiarch support
%ifarch %{multiarchs}
  mv %{buildroot}/%{_bindir}/mysql_config %{buildroot}/%{_bindir}/mysql_config-%{__isa_bits}
  install -p -m 0755 %{SOURCE5} %{buildroot}/%{_bindir}/mysql_config
%endif

%if 0%{?systemd}
install -D -p -m 0644 scripts/mysqlrouter.service %{buildroot}%{_unitdir}/mysqlrouter.service
#install -D -p -m 0644 packaging/rpm-common/mysqlrouter.conf %{buildroot}%{_tmpfilesdir}/mysqlrouter.conf
#install -D -p -m 0644 packaging/rpm-common/mysqlrouter.tmpfiles.d %{buildroot}%{_tmpfilesdir}/mysqlrouter.conf
%else
install -D -p -m 0755 packaging/rpm-common/mysqlrouter.init %{buildroot}%{_sysconfdir}/init.d/mysqlrouter
%endif
install -D -p -m 0644 packaging/rpm-common/mysqlrouter.conf %{buildroot}%{_sysconfdir}/mysqlrouter/mysqlrouter.conf

# set rpath for plugin to use private/libfido2.so
#patchelf --debug --set-rpath '$ORIGIN/../private' %{buildroot}/%{_libdir}/mysql/plugin/authentication_fido.so

# Remove files pages we explicitly do not want to package
rm -rf %{buildroot}%{_infodir}/mysql.info*
rm -rf %{buildroot}%{_datadir}/greatsql/mysql.server
rm -rf %{buildroot}%{_datadir}/greatsql/mysqld_multi.server
rm -f %{buildroot}%{_datadir}/greatsql/win_install_firewall.sql
rm -f %{buildroot}%{_datadir}/greatsql/audit_log_filter_win_install.sql
rm -rf %{buildroot}%{_bindir}/mysql_embedded
rm -rf %{buildroot}/usr/cmake/coredumper-relwithdebinfo.cmake
rm -rf %{buildroot}/usr/cmake/coredumper.cmake
rm -rf %{buildroot}/usr/include/kmip.h
rm -rf %{buildroot}/usr/include/kmippp.h
rm -rf %{buildroot}/usr/lib/libkmip.a
rm -rf %{buildroot}/usr/lib/libkmippp.a
%if 0%{?tokudb}
  rm -f %{buildroot}%{_prefix}/README.md
  rm -f %{buildroot}%{_prefix}/COPYING.AGPLv3
  rm -f %{buildroot}%{_prefix}/COPYING.GPLv2
  rm -f %{buildroot}%{_prefix}/PATENTS
%endif

# Remove upcoming man pages, to avoid breakage when they materialize
# Keep this comment as a placeholder for future cases
# rm -f %{buildroot}%{_mandir}/man1/<manpage>.1

# Remove removed manpages here until they are removed from the docs repo

%check
%if 0%{?runselftest}
  pushd release
    make test VERBOSE=1
    export MTR_BUILD_THREAD=auto
  pushd mysql-test
  ./mtr \
    --mem --parallel=auto --force --retry=0 \
    --mysqld=--binlog-format=mixed \
    --suite-timeout=720 --testcase-timeout=30 \
    --clean-vardir
  rm -r $(readlink var) var
%endif

%pretrans -n greatsql-server
if [ -d %{_datadir}/mysql ] && [ ! -L %{_datadir}/mysql ]; then
  MYCNF_PACKAGE=$(rpm -qf /usr/share/mysql --queryformat "%{NAME}")
fi

if [ "$MYCNF_PACKAGE" == "mariadb-libs" -o "$MYCNF_PACKAGE" == "mysql-libs" ]; then
  MODIFIED=$(rpm -Va "$MYCNF_PACKAGE" | grep '/usr/share/mysql' | awk '{print $1}' | grep -c 5)
  if [ "$MODIFIED" == 1 ]; then
    cp -r %{_datadir}/mysql %{_datadir}/mysql.old
  fi
fi

%pre -n greatsql-server
/usr/sbin/groupadd -g 27 -o -r mysql >/dev/null 2>&1 || :
/usr/sbin/useradd -M %{!?el5:-N} -g mysql -o -r -d /var/lib/mysql -s /bin/false \
    -c "GreatSQL" -u 27 mysql >/dev/null 2>&1 || :
if [ "$1" = 1 ]; then
  if [ -f %{_sysconfdir}/my.cnf ]; then
    timestamp=$(date '+%Y%m%d-%H%M')
    cp %{_sysconfdir}/my.cnf \
    %{_sysconfdir}/my.cnf.rpmsave-${timestamp}
  fi
fi

%post -n greatsql-server
datadir=$(/usr/bin/my_print_defaults server mysqld | grep '^--datadir=' | sed -n 's/--datadir=//p' | tail -n 1)
/bin/chmod 0751 "$datadir" >/dev/null 2>&1 || :
if [ ! -e /var/log/mysqld.log ]; then
    /usr/bin/install -m0640 -omysql -gmysql /dev/null /var/log/mysqld.log
fi
#/bin/touch /var/log/mysqld.log >/dev/null 2>&1 || :
%if 0%{?systemd}
  %systemd_post mysqld.service
  if [ $1 == 1 ]; then
      /usr/bin/systemctl enable mysqld >/dev/null 2>&1 || :
  fi
%else
  if [ $1 == 1 ]; then
      /sbin/chkconfig --add mysql
  fi
%endif

if [ -d /etc/greatsql.conf.d ]; then
    CONF_EXISTS=$(grep "greatsql.conf.d" /etc/my.cnf | wc -l)
    if [ ${CONF_EXISTS} = 0 ]; then
        echo "!includedir /etc/greatsql.conf.d/" >> /etc/my.cnf
    fi
fi
echo "datadir=/var/lib/mysql" >> /etc/my.cnf
echo "socket=/var/lib/mysql/mysql.sock" >> /etc/my.cnf
echo "log-error=/var/log/mysqld.log" >> /etc/my.cnf
echo "pid-file=/var/run/mysqld/mysqld.pid" >> /etc/my.cnf
echo "slow_query_log = ON" >> /etc/my.cnf
echo "long_query_time = 1" >> /etc/my.cnf
echo "log_slow_verbosity = FULL" >> /etc/my.cnf
echo "log_error_verbosity = 3" >> /etc/my.cnf
echo "innodb_buffer_pool_size = 1G" >> /etc/my.cnf
echo "innodb_log_file_size = 128M" >> /etc/my.cnf

%preun -n greatsql-server
%if 0%{?systemd}
  %systemd_preun mysqld.service
%else
  if [ "$1" = 0 ]; then
    /sbin/service mysql stop >/dev/null 2>&1 || :
    /sbin/chkconfig --del mysql
  fi
%endif
if [ "$1" = 0 ]; then
  if [ -L %{_datadir}/mysql ]; then
      rm %{_datadir}/mysql
  fi
  if [ -f %{_sysconfdir}/my.cnf ]; then
    cp %{_sysconfdir}/my.cnf \
    %{_sysconfdir}/my.cnf.rpmsave
  fi
fi

%postun -n greatsql-server
%if 0%{?systemd}
  %systemd_postun_with_restart mysqld.service
%else
  if [ $1 -ge 1 ]; then
    /sbin/service mysql condrestart >/dev/null 2>&1 || :
  fi
%endif

%posttrans -n greatsql-server
if [ -d %{_datadir}/mysql ] && [ ! -L %{_datadir}/mysql ]; then
  MYCNF_PACKAGE=$(rpm -qf /usr/share/mysql --queryformat "%{NAME}")
  if [ "$MYCNF_PACKAGE" == "file %{_datadir}/mysql is not owned by any package" ]; then
    mv %{_datadir}/mysql %{_datadir}/mysql.old
  fi
fi

if [ ! -d %{_datadir}/mysql ] && [ ! -L %{_datadir}/mysql ]; then
    ln -s %{_datadir}/greatsql %{_datadir}/mysql
fi

%post -n greatsql-shared -p /sbin/ldconfig

%postun -n greatsql-shared -p /sbin/ldconfig

%if 0%{?tokudb}
%post -n greatsql-tokudb
if [ $1 -eq 1 ] ; then
  echo -e "\n\n * This release of GreatSQL is distributed with TokuDB storage engine."
  echo -e " * Run the following script to enable the TokuDB storage engine in Percona Server:\n"
  echo -e "\tps-admin --enable-tokudb -u <mysql_admin_user> -p[mysql_admin_pass] [-S <socket>] [-h <host> -P <port>]\n"
  echo -e " * See http://www.percona.com/doc/percona-server/8.0/tokudb/tokudb_installation.html for more installation details\n"
  echo -e " * See http://www.percona.com/doc/percona-server/8.0/tokudb/tokudb_intro.html for an introduction to TokuDB\n\n"
fi
%endif

%if 0%{?rocksdb}
%post -n greatsql-rocksdb
if [ $1 -eq 1 ] ; then
  echo -e "\n\n * This release of GreatSQL is distributed with RocksDB storage engine."
  echo -e " * Run the following script to enable the RocksDB storage engine in GreatSQL:\n"
  echo -e "\tps-admin --enable-rocksdb -u <mysql_admin_user> -p[mysql_admin_pass] [-S <socket>] [-h <host> -P <port>]\n"
fi
%endif

%pre -n greatsql-mysql-router
/usr/sbin/groupadd -r mysqlrouter >/dev/null 2>&1 || :
/usr/sbin/useradd -M -N -g mysqlrouter -r -d /var/lib/mysqlrouter -s /bin/false \
    -c "GreatSQL MySQL Router" mysqlrouter >/dev/null 2>&1 || :

%post -n greatsql-mysql-router
/sbin/ldconfig
%if 0%{?systemd}
%systemd_post mysqlrouter.service
%else
/sbin/chkconfig --add mysqlrouter
%endif # systemd

%preun -n greatsql-mysql-router
%if 0%{?systemd}
%systemd_preun mysqlrouter.service
%else
if [ "$1" = 0 ]; then
    /sbin/service mysqlrouter stop >/dev/null 2>&1 || :
    /sbin/chkconfig --del mysqlrouter
fi
%endif # systemd

%postun -n greatsql-mysql-router
/sbin/ldconfig
%if 0%{?systemd}
%systemd_postun_with_restart mysqlrouter.service
%else
if [ $1 -ge 1 ]; then
    /sbin/service mysqlrouter condrestart >/dev/null 2>&1 || :
fi
%endif # systemd


%files -n greatsql-server
%defattr(-, root, root, -)
%doc %{?license_files_server}
%doc %{src_dir}/Docs/INFO_SRC*
%doc release/Docs/INFO_BIN*
%attr(644, root, root) %{_mandir}/man1/innochecksum.1*
%attr(644, root, root) %{_mandir}/man1/ibd2sdi.1*
%attr(644, root, root) %{_mandir}/man1/my_print_defaults.1*
%attr(644, root, root) %{_mandir}/man1/myisam_ftdump.1*
%attr(644, root, root) %{_mandir}/man1/myisamchk.1*
%attr(644, root, root) %{_mandir}/man1/myisamlog.1*
%attr(644, root, root) %{_mandir}/man1/myisampack.1*
%attr(644, root, root) %{_mandir}/man8/mysqld.8*
%if 0%{?systemd}
%exclude %{_mandir}/man1/mysqld_multi.1*
%exclude %{_mandir}/man1/mysqld_safe.1*
%else
%attr(644, root, root) %{_mandir}/man1/mysqld_multi.1*
%attr(644, root, root) %{_mandir}/man1/mysqld_safe.1*
%endif
%attr(644, root, root) %{_mandir}/man1/mysqldumpslow.1*
%attr(644, root, root) %{_mandir}/man1/mysql_secure_installation.1*
%attr(644, root, root) %{_mandir}/man1/mysql_upgrade.1*
%attr(644, root, root) %{_mandir}/man1/mysqlman.1*
#%attr(644, root, root) %{_mandir}/man1/mysql.server.1*
%attr(644, root, root) %{_mandir}/man1/mysql_tzinfo_to_sql.1*
%attr(644, root, root) %{_mandir}/man1/perror.1*
%attr(644, root, root) %{_mandir}/man1/mysql_ssl_rsa_setup.1*
%attr(644, root, root) %{_mandir}/man1/lz4_decompress.1*
%attr(644, root, root) %{_mandir}/man1/zlib_decompress.1*

%config(noreplace) %{_sysconfdir}/my.cnf
%dir %{_sysconfdir}/my.cnf.d

%attr(755, root, root) %{_bindir}/comp_err
%attr(755, root, root) %{_bindir}/innochecksum
%attr(755, root, root) %{_bindir}/ibd2sdi
%attr(755, root, root) %{_bindir}/my_print_defaults
%attr(755, root, root) %{_bindir}/myisam_ftdump
%attr(755, root, root) %{_bindir}/myisamchk
%attr(755, root, root) %{_bindir}/myisamlog
%attr(755, root, root) %{_bindir}/myisampack
%attr(755, root, root) %{_bindir}/mysql_secure_installation
%attr(755, root, root) %{_bindir}/mysql_tzinfo_to_sql
%attr(755, root, root) %{_bindir}/mysql_upgrade
%attr(755, root, root) %{_bindir}/mysqldumpslow
%attr(755, root, root) %{_bindir}/ps_mysqld_helper
%attr(755, root, root) %{_bindir}/perror
%attr(755, root, root) %{_bindir}/mysql_ssl_rsa_setup
%attr(755, root, root) %{_bindir}/lz4_decompress
%attr(755, root, root) %{_bindir}/zlib_decompress
%attr(755, root, root) %{_bindir}/ps-admin
%if 0%{?systemd}
%attr(755, root, root) %{_bindir}/mysqld_pre_systemd
%attr(755, root, root) %{_bindir}/mysqld_safe
%else
%attr(755, root, root) %{_bindir}/mysqld_multi
%attr(755, root, root) %{_bindir}/mysqld_safe
%endif
%attr(755, root, root) %{_sbindir}/mysqld
%attr(755, root, root) %{_sbindir}/mysqld-debug
%dir %{_libdir}/mysql/private
%attr(755, root, root) %{_libdir}/mysql/private/libprotobuf-lite.so.*
%attr(755, root, root) %{_libdir}/mysql/private/libprotobuf.so.*

%dir %{_libdir}/mysql/plugin
%attr(755, root, root) %{_libdir}/mysql/plugin/procfs.so
%attr(755, root, root) %{_libdir}/mysql/plugin/binlog_utils_udf.so
%attr(755, root, root) %{_libdir}/mysql/plugin/adt_null.so
%attr(755, root, root) %{_libdir}/mysql/plugin/auth_socket.so
%attr(755, root, root) %{_libdir}/mysql/plugin/authentication_kerberos_client.so
%attr(755, root, root) %{_libdir}/mysql/plugin/authentication_ldap_sasl.so
%attr(755, root, root) %{_libdir}/mysql/plugin/authentication_ldap_sasl_client.so
%attr(755, root, root) %{_libdir}/mysql/plugin/authentication_ldap_simple.so
%attr(755, root, root) %{_libdir}/mysql/plugin/authentication_oci_client.so
%attr(755, root, root) %{_libdir}/mysql/plugin/greatdb_ha.so
%attr(755, root, root) %{_libdir}/mysql/plugin/group_replication.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_audit_api_message_emit.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_encryption_udf.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_keyring_file.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_keyring_kmip.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_keyring_kms.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_log_filter_dragnet.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_log_sink_json.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_log_sink_syseventlog.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_mysqlbackup.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_query_attributes.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_reference_cache.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_audit_api_message.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_component_deinit.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_host_application_signal.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_mysql_command_services.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_mysql_system_variable_set.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_sensitive_system_variables.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_status_var_reader.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_table_access.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_udf_services.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_validate_password.so
%attr(755, root, root) %{_libdir}/mysql/plugin/conflicting_variables.so
%attr(755, root, root) %{_libdir}/mysql/plugin/connection_control.so
%attr(755, root, root) %{_libdir}/mysql/plugin/ddl_rewriter.so
%attr(755, root, root) %{_libdir}/mysql/plugin/ha_example.so
%attr(755, root, root) %{_libdir}/mysql/plugin/ha_mock.so
%attr(755, root, root) %{_libdir}/mysql/plugin/keyring_file.so
%attr(755, root, root) %{_libdir}/mysql/plugin/keyring_udf.so
%attr(755, root, root) %{_libdir}/mysql/plugin/innodb_engine.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libmemcached.so
%attr(755, root, root) %{_libdir}/mysql/plugin/locking_service.so
%attr(755, root, root) %{_libdir}/mysql/plugin/mypluglib.so
%attr(755, root, root) %{_libdir}/mysql/plugin/mysql_clone.so
%attr(755, root, root) %{_libdir}/mysql/plugin/mysql_no_login.so
%attr(755, root, root) %{_libdir}/mysql/plugin/rewrite_example.so
%attr(755, root, root) %{_libdir}/mysql/plugin/rewriter.so
%attr(755, root, root) %{_libdir}/mysql/plugin/semisync_master.so
%attr(755, root, root) %{_libdir}/mysql/plugin/semisync_slave.so
%attr(755, root, root) %{_libdir}/mysql/plugin/semisync_replica.so
%attr(755, root, root) %{_libdir}/mysql/plugin/semisync_source.so
%attr(755, root, root) %{_libdir}/mysql/plugin/validate_password.so
%attr(755, root, root) %{_libdir}/mysql/plugin/version_token.so
%attr(755, root, root) %{_libdir}/mysql/plugin/test_services_command_services.so
%attr(755, root, root) %{_libdir}/mysql/plugin/test_services_host_application_signal.so
%attr(755, root, root) %{_libdir}/mysql/plugin/test_udf_wrappers.so
%attr(755, root, root) %{_libdir}/mysql/plugin/data_masking*

%dir %{_libdir}/mysql/plugin/debug
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/procfs.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/data_masking.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/adt_null.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/auth_socket.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/authentication_kerberos_client.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/authentication_ldap_simple.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/authentication_ldap_sasl.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/authentication_ldap_sasl_client.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/authentication_oci_client.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/greatdb_ha.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/group_replication.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_audit_api_message_emit.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_encryption_udf.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_keyring_file.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_keyring_kmip.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_keyring_kms.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_log_filter_dragnet.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_log_sink_json.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_log_sink_syseventlog.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_mysqlbackup.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_query_attributes.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_reference_cache.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_audit_api_message.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_component_deinit.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_host_application_signal.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_mysql_command_services.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_mysql_system_variable_set.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_sensitive_system_variables.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_status_var_reader.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_table_access.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_udf_services.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_validate_password.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/conflicting_variables.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/connection_control.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/ddl_rewriter.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/ha_example.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/ha_mock.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/keyring_file.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/keyring_udf.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/innodb_engine.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libmemcached.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/locking_service.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/mypluglib.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/mysql_clone.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/mysql_no_login.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/rewrite_example.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/rewriter.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/semisync_master.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/semisync_slave.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/semisync_replica.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/semisync_source.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/validate_password.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/version_token.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/test_services_command_services.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/test_services_host_application_signal.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/binlog_utils_udf.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/test_udf_wrappers.so

%if 0%{?mecab}
%{_libdir}/mysql/mecab
%attr(755, root, root) %{_libdir}/mysql/plugin/libpluginmecab.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libpluginmecab.so
%endif
#coredumper
%attr(755, root, root) %{_includedir}/coredumper/coredumper.h
%attr(755, root, root) /usr/lib/libcoredumper.a
# Percona plugins
%attr(755, root, root) %{_libdir}/mysql/plugin/audit_log.so
#%attr(644, root, root) %{_datadir}/mysql-*/audit_log_filter_linux_install.sql
#%attr(755, root, root) %{_libdir}/mysql/plugin/authentication_pam.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/authentication_ldap_sasl.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/authentication_ldap_simple.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/keyring_okv.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/keyring_encrypted_file.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/mysql_clone.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/thread_pool.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/openssl_udf.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/firewall.so
#%attr(644, root, root) %{_datadir}/mysql-*/linux_install_firewall.sql
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/audit_log.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/scalability_metrics.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/debug/scalability_metrics.so
%attr(755, root, root) %{_libdir}/mysql/plugin/auth_pam.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/auth_pam.so
%attr(755, root, root) %{_libdir}/mysql/plugin/auth_pam_compat.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/auth_pam_compat.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libfnv1a_udf.*
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libfnv1a_udf.*
%attr(755, root, root) %{_libdir}/mysql/plugin/libfnv_udf.*
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libfnv_udf.*
%attr(755, root, root) %{_libdir}/mysql/plugin/libmurmur_udf.*
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libmurmur_udf.*
%attr(755, root, root) %{_libdir}/mysql/plugin/dialog.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/dialog.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/query_response_time.so
#%attr(755, root, root) %{_libdir}/mysql/plugin/debug/query_response_time.so
%attr(755, root, root) %{_libdir}/mysql/plugin/keyring_vault.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/keyring_vault.so
#
#%attr(644, root, root) %{_datadir}/greatsql/fill_help_tables.sql
#%attr(644, root, root) %{_datadir}/greatsql/mysql_sys_schema.sql
#%attr(644, root, root) %{_datadir}/greatsql/mysql_system_tables.sql
#%attr(644, root, root) %{_datadir}/greatsql/mysql_system_tables_data.sql
#%attr(644, root, root) %{_datadir}/greatsql/mysql_test_data_timezone.sql
%attr(644, root, root) %{_datadir}/greatsql/mysql-log-rotate
#%attr(644, root, root) %{_datadir}/greatsql/mysql_security_commands.sql
%attr(644, root, root) %{_datadir}/greatsql/dictionary.txt
%attr(644, root, root) %{_datadir}/greatsql/innodb_memcached_config.sql
%attr(644, root, root) %{_datadir}/greatsql/install_rewriter.sql
%attr(644, root, root) %{_datadir}/greatsql/uninstall_rewriter.sql
%if 0%{?systemd}
%attr(644, root, root) %{_unitdir}/mysqld.service
%attr(644, root, root) %{_unitdir}/mysqld@.service
%attr(644, root, root) %{_prefix}/lib/tmpfiles.d/mysql.conf
%else
%attr(755, root, root) %{_sysconfdir}/init.d/mysql
%endif
%attr(644, root, root) %config(noreplace,missingok) %{_sysconfdir}/logrotate.d/mysql
%dir %attr(751, mysql, mysql) /var/lib/mysql
%dir %attr(755, mysql, mysql) /var/run/mysqld
%dir %attr(750, mysql, mysql) /var/lib/mysql-files
%dir %attr(750, mysql, mysql) /var/lib/mysql-keyring

%attr(755, root, root) %{_datadir}/greatsql/messages_to_clients.txt
%attr(755, root, root) %{_datadir}/greatsql/messages_to_error_log.txt
%attr(755, root, root) %{_datadir}/greatsql/charsets/
%attr(755, root, root) %{_datadir}/greatsql/bulgarian/
%attr(755, root, root) %{_datadir}/greatsql/chinese/
%attr(755, root, root) %{_datadir}/greatsql/czech/
%attr(755, root, root) %{_datadir}/greatsql/danish/
%attr(755, root, root) %{_datadir}/greatsql/dutch/
%attr(755, root, root) %{_datadir}/greatsql/english/
%attr(755, root, root) %{_datadir}/greatsql/estonian/
%attr(755, root, root) %{_datadir}/greatsql/french/
%attr(755, root, root) %{_datadir}/greatsql/german/
%attr(755, root, root) %{_datadir}/greatsql/greek/
%attr(755, root, root) %{_datadir}/greatsql/hungarian/
%attr(755, root, root) %{_datadir}/greatsql/italian/
%attr(755, root, root) %{_datadir}/greatsql/japanese/
%attr(755, root, root) %{_datadir}/greatsql/korean/
%attr(755, root, root) %{_datadir}/greatsql/norwegian-ny/
%attr(755, root, root) %{_datadir}/greatsql/norwegian/
%attr(755, root, root) %{_datadir}/greatsql/polish/
%attr(755, root, root) %{_datadir}/greatsql/portuguese/
%attr(755, root, root) %{_datadir}/greatsql/romanian/
%attr(755, root, root) %{_datadir}/greatsql/russian/
%attr(755, root, root) %{_datadir}/greatsql/serbian/
%attr(755, root, root) %{_datadir}/greatsql/slovak/
%attr(755, root, root) %{_datadir}/greatsql/spanish/
%attr(755, root, root) %{_datadir}/greatsql/swedish/
%attr(755, root, root) %{_datadir}/greatsql/ukrainian/
#%attr(755, root, root) %{_datadir}/greatsql/mysql_system_users.sql

%files -n greatsql-client
%defattr(-, root, root, -)
%doc %{?license_files_server}
%attr(755, root, root) %{_bindir}/mysql
%attr(755, root, root) %{_bindir}/mysqladmin
%attr(755, root, root) %{_bindir}/mysqlbinlog
%attr(755, root, root) %{_bindir}/mysqlcheck
%attr(755, root, root) %{_bindir}/mysqldecrypt
%attr(755, root, root) %{_bindir}/mysqldump
%attr(755, root, root) %{_bindir}/mysqlimport
%attr(755, root, root) %{_bindir}/mysqlpump
%attr(755, root, root) %{_bindir}/mysqlshow
%attr(755, root, root) %{_bindir}/mysqlslap
%attr(755, root, root) %{_bindir}/mysql_config_editor
%attr(755, root, root) %{_bindir}/mysql_migrate_keyring
%attr(755, root, root) %{_bindir}/mysql_keyring_encryption_test

%attr(644, root, root) %{_mandir}/man1/mysql.1*
%attr(644, root, root) %{_mandir}/man1/mysqladmin.1*
%attr(644, root, root) %{_mandir}/man1/mysqlbinlog.1*
%attr(644, root, root) %{_mandir}/man1/mysqlcheck.1*
%attr(644, root, root) %{_mandir}/man1/mysqldump.1*
%attr(644, root, root) %{_mandir}/man1/mysqlpump.1*
%attr(644, root, root) %{_mandir}/man1/mysqlimport.1*
%attr(644, root, root) %{_mandir}/man1/mysqlshow.1*
%attr(644, root, root) %{_mandir}/man1/mysqlslap.1*
%attr(644, root, root) %{_mandir}/man1/mysql_config_editor.1*

%files -n greatsql-devel
%defattr(-, root, root, -)
%doc %{?license_files_server}
%attr(644, root, root) %{_mandir}/man1/comp_err.1*
%attr(644, root, root) %{_mandir}/man1/mysql_config.1*
%attr(755, root, root) %{_bindir}/mysql_config
%attr(755, root, root) %{_bindir}/mysql_config-%{__isa_bits}
%{_includedir}/mysql
%{_datadir}/aclocal/mysql.m4
%{_libdir}/mysql/lib%{shared_lib_pri_name}.a
%{_libdir}/mysql/libmysqlservices.a
%{_libdir}/mysql/lib%{shared_lib_pri_name}.so
%{_libdir}/pkgconfig/%{shared_lib_pri_name}.pc

%files -n greatsql-shared
%defattr(-, root, root, -)
%doc %{?license_files_server}
%dir %attr(755, root, root) %{_libdir}/mysql
%attr(644, root, root) %{_sysconfdir}/ld.so.conf.d/mysql-%{_arch}.conf
%{_libdir}/mysql/lib%{shared_lib_pri_name}.so.21*

%files -n greatsql-test
%defattr(-, root, root, -)
%doc %{?license_files_server}
%attr(-, root, root) %{_datadir}/mysql-test
%attr(755, root, root) %{_bindir}/mysql_client_test
%attr(755, root, root) %{_bindir}/mysqltest
%attr(755, root, root) %{_bindir}/mysqltest_safe_process
%attr(755, root, root) %{_bindir}/mysqlxtest

%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_sleep_is_connected.so
%attr(755, root, root) %{_libdir}/mysql/plugin/auth.so
%attr(755, root, root) %{_libdir}/mysql/plugin/auth_test_plugin.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_example_component1.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_example_component2.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_example_component3.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_log_sink_test.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_backup_lock_service.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_string_service_charset.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_string_service_long.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_string_service.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_pfs_example.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_pfs_example_component_population.so
%attr(755, root, root) %{_libdir}/mysql/plugin/pfs_example_plugin_employee.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_pfs_notification.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_pfs_resource_group.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_udf_registration.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_mysql_current_thread_reader.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_udf_reg_3_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_udf_reg_avg_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_udf_reg_int_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_udf_reg_int_same_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_udf_reg_only_3_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_udf_reg_real_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_udf_unreg_3_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_udf_unreg_int_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_udf_unreg_real_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_sys_var_service_int.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_sys_var_service.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_sys_var_service_same.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_sys_var_service_str.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_status_var_service.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_status_var_service_int.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_status_var_service_reg_only.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_status_var_service_str.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_status_var_service_unreg_only.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_system_variable_source.so
%attr(644, root, root) %{_libdir}/mysql/plugin/daemon_example.ini
%attr(755, root, root) %{_libdir}/mysql/plugin/libdaemon_example.so
%attr(755, root, root) %{_libdir}/mysql/plugin/replication_observers_example_plugin.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_framework.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_services.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_services_threaded.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_session_detach.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_session_attach.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_session_in_thd.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_session_info.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_2_sessions.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_all_col_types.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_cmds_1.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_commit.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_complex.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_errors.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_lock.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_processlist.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_replication.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_shutdown.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_stmt.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_sqlmode.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_stored_procedures_functions.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_views_triggers.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_x_sessions_deinit.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_x_sessions_init.so
%attr(755, root, root) %{_libdir}/mysql/plugin/qa_auth_client.so
%attr(755, root, root) %{_libdir}/mysql/plugin/qa_auth_interface.so
%attr(755, root, root) %{_libdir}/mysql/plugin/qa_auth_server.so
%attr(755, root, root) %{_libdir}/mysql/plugin/test_security_context.so
%attr(755, root, root) %{_libdir}/mysql/plugin/test_services_plugin_registry.so
%attr(755, root, root) %{_libdir}/mysql/plugin/test_udf_services.so
%attr(755, root, root) %{_libdir}/mysql/plugin/udf_example.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_mysqlx_global_reset.so
%attr(755, root, root) %{_libdir}/mysql/plugin/component_test_mysql_runtime_error.so
%attr(755, root, root) %{_libdir}/mysql/plugin/libtest_sql_reset_connection.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_mysql_runtime_error.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_reset_connection.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/auth.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/auth_test_plugin.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_example_component1.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_example_component2.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_example_component3.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_log_sink_test.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_backup_lock_service.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_string_service_charset.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_string_service_long.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_string_service.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_pfs_example.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_pfs_example_component_population.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/pfs_example_plugin_employee.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_pfs_notification.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_pfs_resource_group.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_udf_registration.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_mysql_current_thread_reader.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_udf_reg_3_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_udf_reg_avg_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_udf_reg_int_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_udf_reg_int_same_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_udf_reg_only_3_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_udf_reg_real_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_udf_unreg_3_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_udf_unreg_int_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_udf_unreg_real_func.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_sys_var_service_int.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_sys_var_service.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_sys_var_service_same.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_sys_var_service_str.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_status_var_service.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_status_var_service_int.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_status_var_service_reg_only.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_status_var_service_str.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_status_var_service_unreg_only.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_test_system_variable_source.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libdaemon_example.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/replication_observers_example_plugin.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_framework.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_services.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_services_threaded.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_session_detach.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_session_attach.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_session_in_thd.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_session_info.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_2_sessions.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_all_col_types.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_cmds_1.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_commit.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_complex.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_errors.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_lock.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_processlist.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_replication.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_shutdown.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_stmt.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_sqlmode.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_stored_procedures_functions.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_views_triggers.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_x_sessions_deinit.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_x_sessions_init.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/qa_auth_client.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/qa_auth_interface.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/qa_auth_server.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/test_security_context.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/test_services_plugin_registry.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/test_udf_services.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/udf_example.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/component_mysqlx_global_reset.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/libtest_sql_sleep_is_connected.so

%if 0%{?tokudb}
%files -n greatsql-tokudb
%attr(-, root, root)
%{_bindir}/tokuftdump
%{_libdir}/mysql/plugin/ha_tokudb.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/ha_tokudb.so
%attr(755, root, root) %{_bindir}/tokuft_logprint
%attr(755, root, root) %{_libdir}/mysql/plugin/tokudb_backup.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/tokudb_backup.so
%attr(755, root, root) %{_libdir}/mysql/libHotBackup.so
%{_includedir}/backup.h
%endif

%if 0%{?rocksdb}
%files -n greatsql-rocksdb
%attr(-, root, root)
%{_libdir}/mysql/plugin/ha_rocksdb.so
%attr(755, root, root) %{_libdir}/mysql/plugin/debug/ha_rocksdb.so
%attr(755, root, root) %{_bindir}/ldb
%attr(755, root, root) %{_bindir}/mysql_ldb
%attr(755, root, root) %{_bindir}/sst_dump
%endif

%files -n greatsql-mysql-router
%defattr(-, root, root, -)
%doc %{src_dir}/router/README.router  %{src_dir}/router/LICENSE.router
%dir %{_sysconfdir}/mysqlrouter
%config(noreplace) %{_sysconfdir}/mysqlrouter/mysqlrouter.conf
%attr(644, root, root) %config(noreplace,missingok) %{_sysconfdir}/logrotate.d/mysqlrouter
%{_bindir}/mysqlrouter
%{_bindir}/mysqlrouter_keyring
%{_bindir}/mysqlrouter_passwd
%{_bindir}/mysqlrouter_plugin_info
%attr(644, root, root) %{_mandir}/man1/mysqlrouter.1*
%attr(644, root, root) %{_mandir}/man1/mysqlrouter_passwd.1*
%attr(644, root, root) %{_mandir}/man1/mysqlrouter_plugin_info.1*
%if 0%{?systemd}
%{_unitdir}/mysqlrouter.service
%{_tmpfilesdir}/mysqlrouter.conf
%else
%{_sysconfdir}/init.d/mysqlrouter
%endif
%{_libdir}/mysqlrouter/private/libmysqlharness*.so.*
%{_libdir}/mysqlrouter/private/libmysqlrouter*.so.*
%{_libdir}/mysqlrouter/private/libmysqlrouter_http.so.*
%{_libdir}/mysqlrouter/private/libmysqlrouter_http_auth_backend.so.*
%{_libdir}/mysqlrouter/private/libmysqlrouter_http_auth_realm.so.*
%{_libdir}/mysqlrouter/private/libprotobuf-lite.so.*
%dir %{_libdir}/mysqlrouter
%dir %{_libdir}/mysqlrouter/private
%{_libdir}/mysqlrouter/*.so
%dir %attr(755, mysqlrouter, mysqlrouter) /var/log/mysqlrouter
%dir %attr(755, mysqlrouter, mysqlrouter) /var/run/mysqlrouter
%attr(644, root, root) %config(noreplace,missingok) %{_sysconfdir}/logrotate.d/mysqlrouter

%files -n greatsql-mysql-config
%config(noreplace) %{_sysconfdir}/my.cnf
%dir %{_sysconfdir}/my.cnf.d

%files -n greatsql-icu-data-files
%defattr(-, root, root, -)
%doc %{?license_files_server}
%dir %attr(755, root, root) %{_libdir}/mysql/private/icudt69l
%{_libdir}/mysql/private/icudt69l/unames.icu
%{_libdir}/mysql/private/icudt69l/brkitr


%changelog
* Wed Jun  7 2023 GreatSQL <greatsql@greatdb.com> - 8.0.32-24.1
- Release GreatSQL-8.0.32-24.1

* Wed Jun  6 2022 GreatSQL <greatsql@greatdb.com> - 8.0.25-16.1
- Release GreatSQL-8.0.25-16.1

* Mon Apr 25 2022 GreatSQL <greatsql@greatdb.com> - 8.0.25-15.1
- Release GreatSQL-8.0.25-15.1
