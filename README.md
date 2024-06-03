# PgReplication-DBTest  

本项目是通过 Java 程序，实现实时获取 PostgreSQL 数据库表中数据变更的示例项目。

本项目仅在 PostgreSQL 9.x 版本测试通过，其它版本请自行测试修改。

<p style="text-align: center">
  <a href="https://choosealicense.com/licenses/mit">
	<img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License">
  </a>
</p>

## 基础知识
### 数据同步方式
PostgreSQL数据库提供了两种复制方式：物理复制和逻辑复制。

#### 物理复制
物理复制是指将主库 WAL 日志的日志页直接发到备机，备机完全应用的一种复制方式。

#### 逻辑复制
PostgreSQL 逻辑复制是事务级别的复制，使用订阅复制槽技术，通过在订阅端回放 WAL 日志中的逻辑条目。


项目是通过 Java 程序，实现 PostgreSQL 的逻辑复制的示例项目。

## PostgreSQL 配置
要使用 PostgreSQL 的逻辑复制功能，首先需要对数据库进行相应的配置以支持逻辑复制功能。

### 1、postgres.conf 中加入以下配置项
修改完毕后重启 PostgreSQL 数据库。
```text
wal_level = logical
max_wal_senders = 10
max_worker_processes = 10
max_replication_slots = 10
```
### 2、在 PostgreSQL 数据库中创建复制账号
假设 账号和密码均为：repuser
```sql
CREATE USER repuser REPLICATION LOGIN
CONNECTION LIMIT 8 ENCRYPTED PASSWORD 'repuser';
```
## 修改数据库连接
编辑 _PostgresConnection.java_ 文件中的数据库连接信息
```java
    private static String URL = "jdbc:postgresql://localhost:5432/postgres";
    private static String USERNAME = "repuser";
    private static String PASSWORD = "repuser";
```

## 编译项目
本项目使用 maven 进行编译，请确保已经正确安装 maven。
```cmd
mvn clean package
```

## 启动测试

测试可以通过以下两种方法中的一种：
### 1、在开发工具中运行项目中的 PgReplicationTest.java 类

### 2、编译后运行 target文件夹中的 jar 文件
```cmd
java -jar target/PgReplicationTest.jar
```

成功运行后，在数据库中插入一条数据进行测试，例如执行如下SQL：
```sql
insert into test (id, name) values ('1','a');
```

程序会输出如下内容：
```text
BEGIN 1051
table public.test: INSERT: id[character varying]:'1' name[character varying]:'a'
COMMIT 1051 (at 2024-06-03 19:07:34.927343+08)
```

## 贡献指南

欢迎各种形式贡献，不仅以代码的形式，还包含：

- 错误报告
- 文件


## 关于我

如果对您有用，就亮个小星星支持一下吧。

更多数据库知识可关注微信公众号：山东Oracle用户组

**作者：** Grainger
**邮箱：** sdfiyon@gmail.com





