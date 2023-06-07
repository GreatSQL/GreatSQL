[![](https://img.shields.io/badge/GreatSQL-官网-orange.svg)](https://greatsql.cn/)
[![](https://img.shields.io/badge/GreatSQL-论坛-brightgreen.svg)](https://greatsql.cn/forum.php)
[![](https://img.shields.io/badge/GreatSQL-博客-brightgreen.svg)](https://greatsql.cn/home.php?mod=space&uid=10&do=blog&view=me&from=space)
[![](https://img.shields.io/badge/License-GPL_v2.0-blue.svg)](https://github.com/GreatSQL/GreatSQL/blob/main/LICENSE)
[![](https://img.shields.io/badge/release-8.0.32_24-blue.svg)](https://github.com/GreatSQL/GreatSQL/releases/tag/GreatSQL-8.0.32-24)

# 关于 GreatSQL
--- 

GreatSQL是适用于金融级应用的国内自主开源数据库，具备高性能、高可靠、高易用性、高安全等多个核心特性，可以作为MySQL或Percona Server的可选替换，用于线上生产环境，且完全免费并兼容MySQL或Percona Server。

![GreatSQL LOGO](/GreatSQL-logo-01.png "GreatSQL LOGO")

# 下载GreatSQL
---

## GreatSQL 8.0
- [GreatSQL 8.0.32-24](https://github.com/GreatSQL/GreatSQL/releases/GreatSQL-8.0.32-24)
- [GreatSQL 8.0.25-17](https://github.com/GreatSQL/GreatSQL/releases/GreatSQL-8.0.25-17)
- [GreatSQL 8.0.25-16](https://github.com/GreatSQL/GreatSQL/releases/GreatSQL-8.0.25-16)
- [GreatSQL 8.0.25-15](https://github.com/GreatSQL/GreatSQL/releases/GreatSQL-8.0.25-15)

## GreatSQL 5.7
- [GreatSQL 5.7.36](https://github.com/GreatSQL/GreatSQL/releases/GreatSQL-5.7.36-39)


# GreatSQL核心特性
---
GreatSQL具备高性能、高可靠、高易用性、高安全等多个核心特性。

**1. 高性能**
- 支持InnoDB并行查询，适用于轻量级OLAP应用场景，在TPC-H测试中平均提升15倍，最高提升40+倍。
- 优化InnoDB事务系统，实现了大锁拆分及无锁化等多种优化方案，OLTP场景整体性能提升约20%。
- 支持并行load data，适用于频繁导入大批量数据的应用场景，性能可提升约20+倍。
- 支持线程池（thread pool），降低了线程创建和销毁的代价，保证高并发下，性能稳定不会明显衰退。


**2. 高可靠**
GreatSQL针对MGR进行了大量改进和提升工作，进一步提升MGR的高可靠等级。
- 地理标签，提升多机房架构数据可靠性。
- 读写节点动态VIP，高可用切换更便捷。
- 仲裁节点，用更低的服务器成本实现更高可用。
- 快速单主模式，在单主模式下更快，性能更高。
- 智能选主，高可用切换选主机制更合理。
- 全新流控算法，使得事务更平稳，避免剧烈抖动。
- 优化了节点加入、退出时可能导致性能剧烈抖动的问题。
- 解决磁盘空间爆满时导致MGR集群阻塞的问题。
- 解决了长事务造成无法选主的问题。
- 优化事务认证队列清理算法，规避每60s抖动问题。
- 修复了recover过程中长时间等待的问题。

**3. 高易用性**
支持多个SQL兼容特性，包括CLOB、VARCHAR2数据类型，DATETIME运算、ROWNUM、子查询无别名、EXPLAIN PLAN FOR等语法，以及ADD_MONTHS()、CAST()、DECODE()等17个函数。

更多信息详见文档：[GreatSQL中的SQL兼容性](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/relnotes/greatsql-803224/sql-compat.md)。

**4. 高安全性**
支持逻辑备份加密、CLONE备份加密、审计日志入表、表空间国密加密等多个安全提升特性，进一步保障业务数据安全，更适用于金融级应用场景。

更多信息详见文档：[GreatSQL中的安全性提升](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/relnotes/greatsql-803224/changes-greatsql-8-0-32-24-20230605.md#14-%E5%AE%89%E5%85%A8)

# 注意事项
---
运行GreatSQL可能需要依赖jemalloc库（推荐5.2.1+版本），因此请先先安装上
```
yum -y install jemalloc jemalloc-devel
```
也可以把自行安装的lib库so文件路径加到系统配置文件中，例如：
```
[root@greatdb]# cat /etc/ld.so.conf
/usr/local/lib64/
```
而后执行下面的操作加载libjemalloc库，并确认是否已存在
```
[root@greatdb]# ldconfig

[root@greatdb]# ldconfig -p | grep libjemalloc
        libjemalloc.so.1 (libc6,x86-64) => /usr/local/lib64/libjemalloc.so.1
        libjemalloc.so (libc6,x86-64) => /usr/local/lib64/libjemalloc.so
```
jemalloc下载地址：https://centos.pkgs.org/8/epel-x86_64/jemalloc-5.2.1-2.el8.x86_64.rpm.html

# 安装GreatSQL
推荐安装GreatSQL RPM包。
[戳此链接下载GreatSQL RPM包](https://github.com/GreatSQL/GreatSQL/releases/GreatSQL-8.0.32-24)。

执行下面的命令安装GreatSQL：
```
#首先，查找GreatSQL是否已安装
$ yum search greatsql
...
No matches found.

#然后安装
$ rpm -ivh --nodeps greatsql-client-8.0.32-24.1.el8.x86_64.rpm greatsql-devel-8.0.32-24.1.el8.x86_64.rpm greatsql-icu-data-files-8.0.32-24.1.el8.x86_64.rpm greatsql-mysql-router-8.0.32-24.1.el8.x86_64.rpm greatsql-server-8.0.32-24.1.el8.x86_64.rpm greatsql-shared-8.0.32-24.1.el8.x86_64.rpm greatsql-test-8.0.32-24.1.el8.x86_64.rpm
```

**提示**：正式安装GreatSQL RPM包时，可能还需要依赖Perl等其他软件包，此处为快速演示，因此加上 `--nodeps` 参数，忽略相应的依赖关系检查。安装完毕后，如果因为依赖关系无法启动，请再行安装相应软件依赖包。

安装完成后，GreatSQL会自行完成初始化，可以再检查是否已加入系统服务或已启动：
```
$ systemctl status mysqld
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

# my.cnf参考

- [my.cnf for GreatSQL 8.0.32-24](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/my.cnf-example-greatsql-8.0.32-24)
- [my.cnf for GreatSQL 8.0.25-17](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/my.cnf-example-greatsql-8.0.25-17)
- [my.cnf for GreatSQL 8.0.25-16](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/my.cnf-example-greatsql-8.0.25-16)
- [my.cnf for GreatSQL 8.0.25-15](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/my.cnf-example-greatsql-8.0.25-15)
- [my.cnf for GreatSQL 5.7.36](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/my.cnf-example-greatsql-5.7.36)

就可以正常启动GreatSQL服务了。

# 编译GreatSQL二进制包

推荐利用Docker环境快速编译GreatSQL二进制包，可参考方法：[编译源码安装GreatSQL](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/user-manual/4-install-guide/4-6-install-with-source-code.md)。

# 版本历史
---
## GreatSQL 8.0
- [GreatSQL 更新说明 8.0.32-24(2023-6-5)](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/relnotes/greatsql-803224/changes-greatsql-8-0-32-24-20230605.md)
- [GreatSQL 更新说明 8.0.25-17(2023-3-13)](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/relnotes/changes-greatsql-8-0-25-17-20230313.md)
- [GreatSQL 更新说明 8.0.25-16(2022-5-16)](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/relnotes/changes-greatsql-8-0-25-16-20220516.md)
- [GreatSQL 更新说明 8.0.25-15(2021-8-26)](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/relnotes/changes-greatsql-8-0-25-20210820.md)

## GreatSQL 5.7
- [GreatSQL 更新说明 5.7.36(2022-4-7)](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/relnotes/changes-greatsql-5-7-36-20220407.md)


# 使用文档
---
- [GreatSQL MGR FAQ](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/GreatSQL-FAQ.md)
- [在Linux下源码编译安装GreatSQL](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/build-greatsql-with-source.md)
- [ansible一键安装GreatSQL 8.0.25并构建MGR集群](https://gitee.com/GreatSQL/GreatSQL-Ansible/wikis/ansible%E4%B8%80%E9%94%AE%E5%AE%89%E8%A3%85GreatSQL%208.0.25%E5%B9%B6%E6%9E%84%E5%BB%BAMGR%E9%9B%86%E7%BE%A4)
- [在Docker中部署GreatSQL并构建MGR集群](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/install-greatsql-with-docker.md)
- [MGR优化配置参考](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/mgr-best-options-ref.md)
- [InnoDB并行查询优化参考](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/innodb-parallel-execute.md)
- [利用GreatSQL部署MGR集群](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/using-greatsql-to-build-mgr-and-node-manage.md)
- [MySQL InnoDB Cluster+GreatSQL部署MGR集群](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/mysql-innodb-cluster-with-greatsql.md)
- [利用systemd管理MySQL单机多实例](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/build-multi-instance-with-systemd.md)
- [麒麟OS+龙芯环境编译GreatSQL](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/docs/build-greatsql-with-source-under-kylin-and-loongson.md)

# 专栏文章
- [深入浅出MGR专栏文章](https://gitee.com/GreatSQL/GreatSQL-Doc/blob/master/deep-dive-mgr)，深入浅出MGR相关知识点、运维管理实操，配合「实战MGR」视频内容食用更佳。
- [GreatSQL文档](https://gitee.com/GreatSQL/GreatSQL-Doc/tree/master/user-manual)，GreatSQL开源分支文档。


# 相关资源
- [GreatSQL-Docker](https://github.com/GreatSQL/GreatSQL-Docker)，在Docker中运行GreatSQL。
- [GreatSQL-Ansible](https://github.com/GreatSQL/GreatSQL-Ansible)，利用ansible一键安装GreatSQL并完成MGR集群部署。

# 问题反馈
---
- [问题反馈 github](https://github.com/GreatSQL/GreatSQL/issues)

# 提示
---
[如果您使用了GreatSQL，请告诉我们。有机会获得精美礼品一份和免费技术支持](https://wj.qq.com/s2/11543483/9e09/)

# 联系我们
---

扫码关注微信公众号

![输入图片说明](https://images.gitee.com/uploads/images/2021/0802/141935_2ea2c196_8779455.jpeg "greatsql社区-wx-qrcode-0.5m.jpg")
