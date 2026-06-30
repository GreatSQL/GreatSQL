[![](https://img.shields.io/badge/GreatSQL-官网-orange.svg)](https://greatsql.cn/)
[![](https://img.shields.io/badge/GreatSQL-论坛-brightgreen.svg)](https://greatsql.cn/forum.php)
[![](https://img.shields.io/badge/GreatSQL-博客-brightgreen.svg)](https://greatsql.cn/home.php?mod=space&uid=10&do=blog&view=me&from=space)
[![](https://img.shields.io/badge/License-GPL_v2.0-blue.svg)](https://gitee.com/GreatSQL/GreatSQL/blob/master/LICENSE)
[![](https://img.shields.io/badge/release-8.4.4_5-blue.svg)](https://gitee.com/GreatSQL/GreatSQL/releases/tag/GreatSQL-8.4.4-5)

最后更新：2026-06-30。

本文档适用版本：GreatSQL 8.4.4-5。

## 关于 GreatSQL

GreatSQL 数据库是一款 **开源免费** 数据库，可在普通硬件上满足金融级应用场景，具有 **高可用**、**高性能**、**高兼容**、**高安全** 等特性，可作为 MySQL 或 Percona Server for MySQL 的理想可选替换。

![GreatSQL LOGO](./greatsql-logo.png "GreatSQL LOGO")

## 下载GreatSQL

- [下载 GreatSQL 最新版本](https://gitee.com/GreatSQL/GreatSQL/releases/GreatSQL-8.4.4-5)
- [下载 GreatSQL 历史版本](https://gitee.com/GreatSQL/GreatSQL/releases/)

## GreatSQL核心特性

### [高可用](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha.html)

针对 MGR 及主从复制进行了大量改进和提升工作，支持 地理标签、仲裁节点、读写动态 VIP、快速单主模式、智能选主 等特性，并针对 流控算法、事务认证队列清理算法、节点加入&退出机制、recovery机制、大事务传输压缩等多个 MGR 底层工作机制算法进行深度优化，进一步提升优化了 MGR 的高可用保障及性能稳定性。

- 支持 [地理标签](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha-mgr-zoneid.html) 特性，提升多机房架构数据可靠性。
- 支持 [仲裁节点](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha-mgr-arbitrator.html) 特性，用更低的服务器成本实现更高可用。
- 支持 [读写动态 VIP](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha-mgr-vip.html) 特性，高可用切换更便捷，更快实现读负载均衡。支持 [当主节点切换时，主动关闭当前活跃连接](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha-mgr-kill-conn-after-switch.html)，缩短应用端不可用时长。。
- 支持 [快速单主模式](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha-mgr-fast-mode.html)，在单主模式下更快，性能更高。
- 支持 [智能选主](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha-mgr-election-mode.html) 特性，高可用切换选主机制更合理。
- 优化 [流控算法](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha-mgr-new-fc.html)，使得事务更平稳，避免剧烈抖动。
- 支持 [记录 MGR 网络通信开销超过阈值的事件](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha-mgr-request-time.html)，用于进一步分析和优化。
- 支持自动选择从最新事务数据的成员节点复制数据，可有效提升 Clone 速度，提高 MGR 的服务可靠性。
- 在主从复制中，从节点向主节点发起 Binlog 读取请求时支持限速控制。
- 优化了 [asynchronous connection failover](https://dev.mysql.com/doc/refman/8.0/en/replication-asynchronous-connection-failover.html) 中的故障检测效率，降低主从复制链路断开的时间，提高整体可用性。
- 支持在跨机房容灾场景中的 [主主双向复制防止回路](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha-repl-server-mode.html) 机制。
- 兼容 [`CHANGE MASTER TO`、`START SLAVE` 等全套旧版主从复制语法与配套状态变量、报错信息](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha-repl-interface-cmd.html)。解决从低版本升级至8.4后旧复制管理语句执行报错问题，大幅降低数据库大版本升级改造工作量。
- 优化了 MGR 节点加入、退出时可能导致性能剧烈抖动的问题。
- 解决了个别节点上磁盘空间爆满时导致MGR集群整体被阻塞的问题。
- 优化了 MGR 事务认证队列清理算法，高负载下不复存在每 60 秒性能抖动问题。
- 解决了 MGR 中长事务造成无法选主的问题。
- 修复了 MGR recovery 过程中长时间等待的问题。
- 优化了MGR大事务传输时压缩超过限制的处理机制。

更多信息详见文档：[高可用](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-2-ha.html)。

### [高性能](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf.html)
相对 MySQL 及 Percona Server For MySQL 的性能表现更稳定优异，支持 Rapid 引擎、Turbo引擎、事务无锁化、并行 LOAD DATA、异步删除大表、线程池、非阻塞式 DDL、NUMA 亲和调度优化 等特性，在 [TPC-C 测试中相对 MySQL 性能提升超过 30%](https://greatsql.cn/docs/8.4.4-5/10-optimize/3-5-benchmark-greatsql-vs-mysql-tpcc-report.html)，在 [TPC-H 测试中的性能表现是 MySQL 的十几倍甚至上百倍](https://greatsql.cn/docs/8.4.4-5/10-optimize/3-3-benchmark-greatsql-tpch-report.html)。

- 支持 [大规模并行、基于内存查询、高压缩比的高性能 Rapid 引擎](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-rapid-engine.html)，可将数据分析性能提升几个数量级。
- 支持 [高性能并行查询引擎Turbo](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-turbo-engine.html)，使GreatSQL具备多线程并发的向量化实时查询功能。并且支持 [Turbo 引擎向量相似度查询](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-vector-search.html)。
- 支持[大事务 binlog 独立落盘优化特性](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-binlog-flush-opt-large-trx.html)，该特性可**降低 30%~70% 的大事务提交延迟，提升 10%~40% 的高并发TPS**，保障系统稳定性。
- 优化 [主从/组复制中从节点的并行复制回放机制](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-parallel-replica.html)，有效减少调度阻塞，提升备节点并行回放吞吐能力，降低复制延迟，增强集群高可用稳定性。
- 支持 [SQL Digest维度的执行计划变更异常捕获功能](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-execplan-baseline.html)，持续采集并缓存执行计划基线信息，通过差分比对识别执行计划变化及存疑SQL，解决数据库重启/升级后执行计划漂移不可见等问题。
- 优化 InnoDB 事务系统，实现了大锁拆分及无锁化等多种优化方案，OLTP 场景整体性能提升约 20%。
- 支持 [并行 LOAD DATA](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-parallel-load.html)，适用于频繁导入大批量数据的应用场景，性能可提升约 20 多倍；对于无显式定义主键的场景亦有优化提升。
- 支持 [异步删除大表](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-async-purge-big-table.html)，提高 InnoDB 引擎运行时性能的稳定性。
- 支持 [线程池](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-thread-pool.html)，降低了线程创建和销毁的代价，保证高并发下，性能稳定不会明显衰退。
- 支持 [非阻塞式 DDL](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-nonblocking-ddl.html)，可以避免数据库因为必须尽快完成 DDL 操作而导致业务请求大量被阻塞的问题。
- 支持 [NUMA 亲和性优化](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf-numa-affinity.html)，通过 NUMA 亲和性调度优化，将前端用户线程和后台线程绑定到固定 NUMA 节点上以提升线程处理性能。

更多信息详见文档：[高性能](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-1-highperf.html)。

### [高兼容](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-3-easyuse.html)

GreatSQL 实现 100% 完全兼容 MySQL 及 Percona Server For MySQL 语法，支持大多数常见 Oracle 语法，包括 [数据类型兼容](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-3-easyuse.html#数据类型兼容)、[函数兼容](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-3-easyuse.html#函数兼容)、[SQL 语法兼容](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-3-easyuse.html#sql语法兼容)、[存储程序兼容](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-3-easyuse.html#存储程序兼容) 等众多兼容扩展用法。

更多信息详见文档：[高兼容](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-3-easyuse.html)。

### [高安全](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-4-security.html)

GreatSQL 支持逻辑备份加密、CLONE 备份加密、审计、表空间国密加密、敏感数据脱敏、存储登录历史等多个安全提升特性，进一步保障业务数据安全，更适用于金融级应用场景。

- 支持 [mysqldump 逻辑备份加密](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-4-security-mysqldump-encrypt.html)，提供了利用 mysqldump 逻辑备份的安全加密需求。
- 支持 [Clone 备份加密](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-4-security-clone-encrypt.html)，提供了利用 Clone 物理备份的安全加密需求。
- 支持 [审计功能](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-4-security-audit.html)，及时记录和发现未授权或不安全行为。
- 支持 [InnoDB 表空间国密加密算法](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-4-security-encrypt-with-gmssl.html)，确保重要数据的加密安全。
- 支持 [基于函数和策略的两种数据脱敏](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-4-security-data-masking.html) 工作方式，保障敏感用户数据查询结果保密性。

通过上述多个安全提升特性，进一步保障业务数据安全。更多信息详见文档：[高安全](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-4-security.html)。

### [其他](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-5-others.html)
- 支持 [Clone 在线全量热备、增备及恢复](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-5-clone-compressed-and-incrment-backup.html)，结合 Binlog 可实现恢复到指定时间点。此外，Clone 备份还支持压缩功能。
- 支持 [InnoDB Page透明压缩采用Zstd算法](https://greatsql.cn/docs/8.4.4-5/5-enhance/5-5-innodb-page-compression.html)，进一步提高数据压缩率，尤其是当有大量长文本重复数据时。
- 支持 **mysqlbinlog 显示实际更改的行数** 特性，在 `mysqlbinlog` 的输出中，加上参数 `-vvv` 后，可以补充展示每个事务的实际影响行数 `affected rows`，增强 binlog 解析的可观测性与运维统计能力。

## 安装 GreatSQL

### 安装 jemalloc（推荐）

运行 GreatSQL 时如果有 jemalloc 支持，则数据库进程的内存分配会更稳定、高效，因此建议安装 jemalloc（非必须）。

如果是ARM环境下，可以不必安装配置 jemalloc 依赖。

以 CentOS 8 x86_64 系统为例，采用类似下面的方法安装 jemalloc 软件包：

```bash
# 先安装 epel 源
yum install -y epel-release

# 再安装jemalloc
yum -y install jemalloc jemalloc-devel
```

也可以把自行安装的动态库so文件路径加到系统配置文件中，例如：

```bash
cat /etc/ld.so.conf
/usr/local/lib64/
```

而后执行下面的操作加载libjemalloc库，并确认是否已存在


```bash
ldconfig && ldconfig -p | grep libjemalloc
```

::: details 查看运行结果
```bash
$ ldconfig && ldconfig -p | grep libjemalloc

...
        libjemalloc.so.1 (libc6,x86-64) => /usr/local/lib64/libjemalloc.so.1
        libjemalloc.so (libc6,x86-64) => /usr/local/lib64/libjemalloc.so
```
:::

如果无法通过 yum 直接安装 jemalloc，可以自行下载 RPM 包，地址：[https://centos.pkgs.org/8/epel-x86_64/jemalloc-5.2.1-2.el8.x86_64.rpm.html](https://centos.pkgs.org/8/epel-x86_64/jemalloc-5.2.1-2.el8.x86_64.rpm.html)

### 下载安装 GreatSQL

推荐安装 GreatSQL RPM 包。

[戳此链接下载 GreatSQL RPM 包](https://gitee.com/GreatSQL/GreatSQL/releases/GreatSQL-8.4.4-5)。

以 CentOS 8 系统为例，采用类似下面的命令安装 GreatSQL：

```bash
# 首先，查找GreatSQL是否已安装
yum search greatsql
...
No matches found.

# 然后安装
rpm -ivh --nodeps greatsql-client-8.4.4-5.1.el8.x86_64.rpm greatsql-devel-8.4.4-5.1.el8.x86_64.rpm greatsql-icu-data-files-8.4.4-5.1.el8.x86_64.rpm greatsql-server-8.4.4-5.1.el8.x86_64.rpm greatsql-shared-8.4.4-5.1.el8.x86_64.rpm
```

::: tip 小贴士

正式安装 GreatSQL RPM 包时，可能还需要依赖 openssl、Perl 等其他软件包，此处为快速演示，因此加上 `--nodeps` 参数，忽略相应的依赖关系检查。安装完毕后，如果因为依赖关系无法启动，请再行安装相应软件依赖包。
:::

安装完成后，GreatSQL 会自行完成初始化，可以再检查是否已加入系统服务或已启动：

```bash
systemctl status mysqld
```

::: details 查看运行结果
```bash
$ systemctl status mysqld

...
● mysqld.service - MySQL Server
   Loaded: loaded (/usr/lib/systemd/system/mysqld.service; enabled; vendor preset: disabled)
...
     Docs: man:mysqld(8)
           http://dev.mysql.com/doc/refman/en/using-systemd.html
  Process: 1137698 ExecStartPre=/usr/bin/mysqld_pre_systemd (code=exited, status=0/SUCCESS)
 Main PID: 1137732 (mysqld)
   Status: "Server is operational"
    Tasks: 39 (limit: 149064)
   Memory: 336.7M
   CGroup: /system.slice/mysqld.service
           └─1137732 /usr/sbin/mysqld
...
```
:::

就可以正常启动 GreatSQL 服务了。

想要 GreatSQL 更高效运行，建议参考这份 my.cnf 配置模板：[my.cnf for GreatSQL 8.4.4-5](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/my.cnf-example-greatsql-8.4.4-5)。

### 编译GreatSQL源码

如果想要用GreatSQL源码编译二进制包，可以利用GreatSQL-Build这个Docker镜像来完成，该项目详见：[GreatSQL-Build Docker镜像](https://gitee.com/GreatSQL/GreatSQL-Docker/tree/master/GreatSQL-Build)。

如果想要自行手动编译GreatSQL源码，可以参考以下几篇文档：

- [在CentOS环境下源码编译安装GreatSQL](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/build-greatsql-with-source.md)
- [在CentOS环境下编译GreatSQL RPM包](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/build-greatsql-rpm-under-centos.md)
- [openEuler、龙蜥Anolis、统信UOS系统下编译GreatSQL二进制包](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/build-greatsql-under-openeuler-anolis-uos.md)
- [在麒麟OS+龙芯环境下源码编译安装GreatSQL](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/build-greatsql-with-source-under-kylin-and-loongson.md)

**提醒**：需要用下载[GreatSQL源码包](https://gitee.com/GreatSQL/GreatSQL/releases)进行编译，而不是直接用`git clone`本项目得到的源码包编译，因为缺少部分子模块的代码。

## 版本历史

戳此查看 [GreatSQL 版本历史](https://greatsql.cn/docs/8.4.4-5/1-docs-intro/1-2-release-history.html)。

## GreatSQL vs MySQL

更多关于 GreatSQL 的优势特性详见：[GreatSQL vs MySQL](https://greatsql.cn/docs/8.4.4-5/1-docs-intro/relnotes/changes-greatsql-8445.html#greatsql-vs-mysql)。 

GreatSQL 8.4.4-5 基于 Percona Server for MySQL 8.4.4-4 版本，它在 MySQL 8.4.4 基础上做了大量的改进和提升以及众多新特性，详情请见：[**Percona Server for MySQL feature comparison**](https://docs.percona.com/percona-server/8.4/feature-comparison.html)，这其中包括线程池、审计、数据脱敏等 MySQL 企业版才有的特性，以及 performance_schema 提升、information_schema 提升、性能和可扩展性提升、用户统计增强、PROCESSLIST 增强、Slow Log 增强等大量改进和提升，这里不一一重复列出。

GreatSQL同时也是gitee（码云）平台上的GVP项目，详见：[https://gitee.com/gvp/database-related](https://gitee.com/gvp/database-related) **数据库相关**类目。

## 许可/Licensing

GreatSQL 致力于保持开源的开放性。GreatSQL 采用 GPLv2 协议。

::: tip 小贴士

[如果您在使用 GreatSQL，请告诉我们，将有机会获得精美礼品和免费技术支持](https://wj.qq.com/s2/11543483/9e09/)。
:::

**扫码关注微信公众号**

![GreatSQL社区微信公众号](https://images.gitee.com/uploads/images/2021/0802/141935_2ea2c196_8779455.jpeg "greatsql社区-wx-qrcode-0.5m.jpg")
