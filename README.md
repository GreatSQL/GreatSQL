> English| [中文](./README_zh.md)

[![](https://img.shields.io/badge/GreatSQL-Website-orange.svg)](https://greatsql.cn/)
[![](https://img.shields.io/badge/GreatSQL-Forum-brightgreen.svg)](https://greatsql.cn/forum.php)
[![](https://img.shields.io/badge/GreatSQL-Blog-brightgreen.svg)](https://greatsql.cn/home.php?mod=space&uid=10&do=blog&view=me&from=space)
[![](https://img.shields.io/badge/License-GPL_v2.0-blue.svg)](https://gitee.com/GreatSQL/GreatSQL/blob/master/LICENSE)
[![](https://img.shields.io/badge/release-8.0.25_16-blue.svg)](https://gitee.com/GreatSQL/GreatSQL/releases/tag/GreatSQL-8.0.25-16)



# About GreatSQL
---

GreatSQL is a branch of Percona Server.

GreatSQL focuses on improving the performance and reliability of MGR (MySQL Group Replication), and fixing some bugs. In addition, GreatSQL also merged two Patches contributed by the Huawei Kunpeng Compute Community, respectively for OLTP and OLAP business scenarios, especially the InnoDB parallel query feature. In the TPC-H test, the performance of aggregate analytical SQL was improved by an average of 15 times, the highest increased by more than 40 times, especially suitable for SAP, financial statistics and other businesses such as periodic data summary reports.

GreatSQL can be used as an alternative to MySQL or Percona Server.

GreatSQL is completely free and compatible with MySQL or Percona Server.


# Download GreatSQL
---
## GreatSQL 8.0
- [GreatSQL 8.0.25-16](https://github.com/GreatSQL/GreatSQL/releases/tag/GreatSQL-8.0.25-16)
- [GreatSQL 8.0.25-15](https://github.com/GreatSQL/GreatSQL/releases/tag/GreatSQL-8.0.25-15)

## GreatSQL 5.7
- [GreatSQL 5.7.36-39](https://github.com/GreatSQL/GreatSQL/releases/tag/GreatSQL-5.7.36-39)


# New features
---
In addition to improving the performance and reliability of MGR, GreatSQL also introduces features such as InnoDB transaction lock optimization and parallel query optimization, as well as numerous BUG fixes.

The main advantages of choosing GreatSQl are as follows:

- Improve the concurrent performance and stability of large transactions in MGR.
- Improve MGR's GC and flow control algorithms, and reduce the amount of data sent each time to avoid performance jitter.
- In the AFTER mode of the MGR, the problem that the node is prone to errors when joining the cluster is fixed.
- In the AFTER mode of the MGR, the principle of majority consistency is adopted to adapt to the network partition scene.
- When the MGR node crashes, the abnormal state of the node can be found faster, effectively reducing the waiting time for the switchover and abnormal state.
- Optimize the InnoDB transaction lock mechanism to effectively improve transaction concurrency performance by at least 10% in high concurrency scenarios.
- Realize the InnoDB parallel query mechanism, which greatly improves the efficiency of aggregate query. In the TPC-H test, it can be increased by more than 40 times, and the average increase is 15 times. Especially suitable for SAP, financial statistics and other businesses such as periodic data summary reports.
- Fixed multiple defects or bugs that may cause data loss, performance jitter, and extremely slow node join recovery in MGR.

# Notes
---
jemalloc library is required, please install it first
```
yum -y install jemalloc jemalloc-devel
```

You can also add the path of the self-installed lib library so file to the system configuration file, for example:
```
[root@greatdb]# cat /etc/ld.so.conf
/usr/local/lib64/
```

Load the libjemalloc library, and confirm whether it already exists
```
[root@greatdb]# ldconfig

[root@greatdb]# ldconfig -p | grep libjemalloc
        libjemalloc.so.1 (libc6,x86-64) => /usr/local/lib64/libjemalloc.so.1
        libjemalloc.so (libc6,x86-64) => /usr/local/lib64/libjemalloc.so
```

Now you can start GreatSQL.

# my.cnf examples
- [my.cnf for GreatSQL 8.0.25-16](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/my.cnf-example-greatsql-8.0.25-16)
- [my.cnf for GreatSQL 8.0.25-15](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/my.cnf-example-greatsql-8.0.25-15)
- [my.cnf for GreatSQL 5.7.36](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/my.cnf-example-greatsql-5.7.36)

# Change logs
---
## GreatSQL 8.0
- [Changes in GreatSQL 8.0.25-16(2022-5-16)](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/relnotes/changes-greatsql-8-0-25-16-20220516.md)
- [Changes in GreatSQL 8.0.25-15(2021-8-26)](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/relnotes/changes-greatsql-8-0-25-20210826.md)

## GreatSQL 5.7
- [Changes in GreatSQL 5.7.36-39(2022-4-7)](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/relnotes/changes-greatsql-5-7-36-39-20210407.md)

# Documentation
---
- [GreatSQL MGR FAQ](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/GreatSQL-FAQ.md)
- [Compile and install GreatSQL from source code under Linux](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/build-greatsql-with-source.md)
- [Using Ansible to install GreatSQL and build an MGR cluster](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/install-greatsql-with-ansible.md)
- [Deploy GreatSQL in Docker and build MGR cluster](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/install-greatsql-with-docker.md)
- [MGR optimization configuration reference](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/mgr-best-options-ref.md)
- [InnoDB parallel query optimization reference](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/innodb-parallel-execute.md)
- [Deploy MGR cluster with GreatSQL](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/using-greatsql-to-build-mgr-and-node-manage.md)
- [MySQL InnoDB Cluster+GreatSQL deployment MGR cluster](https://github.com/GreatSQL/GreatSQL-Doc/blob/main/docs/mysql-innodb-cluster-with-greatsql.md)
- [MySQL MGR column article](https://mp.weixin.qq.com/mp/homepage?__biz=MjM5NzAzMTY4NQ==&hid=16&sn=9d3d21966d850dcf158e5b676d9060ed&scene=18#wechat_redirect)

# related documentation
- [GreatSQL-Docker](https://github.com/GreatSQL/GreatSQL-Docker), run GreatSQL in Docker.
- [GreatSQL-Ansible](https://github.com/GreatSQL/GreatSQL-Ansible), use ansible to install GreatSQL with one click and complete the MGR cluster deployment.

# feedback
---
- [Issue feedback](https://github.com/GreatSQL/GreatSQL-Doc/issues)


# contact us
---

Scan QR code to follow WeChat public account

![Enter picture description](https://images.gitee.com/uploads/images/2021/0802/141935_2ea2c196_8779455.jpeg "greatsql community-wx-qrcode-0.5m.jpg")

mail: greatsql@greatdb.com
