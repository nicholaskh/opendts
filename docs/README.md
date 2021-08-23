# dts 

## 背景
> dts（data transfer service），是我们自主研发的一块数据传输系统，旨在通过增量或全量+增量的方式，同步某个数据源的数据到下游的其它数据源。
目前支持MySQL到MySQL，以及MySQL到Kafka，支持增量和全量+增量两种方式。后续会在此基础上支持更多的数据源。

## 使用场景
dts目前可用于TP和AP两种场景。
其中AP类场景，我们通过增量抽取MySQL binlog，并批量回放到Kafka中，再由数据中台研发将Kafka数据回放到hive、flink中，实现MySQL数据到下游AP类存储数据库的数据传输。
AP类场景对数据延迟要求没有那么苛刻，我们在数据抽取和回放的时候做了多级缓冲，通过批量的方式实现数据传输的高吞吐。
AP类场景比较类似Maxwell和Canal Otter。

对于TP类场景，目前主要运用于在线的MySQL到MySQL的数据全量+增量传输，比如同机房单集群的数据同步，以及双活环境。
TP类场景dts还可与dbproxy配合实现在线的数据分片平滑扩容。
后续TP类场景会全量替换阿里云dts。

## 功能介绍

### 基础功能

#### 通用功能
- 支持通过SQL语句指定数据抽取时的filter过滤器
- 支持数字型字段，字符串字段仅支持UTF-8编码
- gh-ost online DDL
- 一个任务涵盖多个MySQL库，多个数据表
- 普通在线DDL

#### OLTP
- MySQL到MySQL数据实时增量同步
- MySQL到MySQL数据全量+增量同步
- MySQL分片平滑扩容（需要配合dbproxy）
- 基于GTID的双向数据同步

#### OLAP
- MySQL到Kafka的增量数据同步
- 指定Kafka Topic，table形式或database形式
- 指定Kafka分片方式，table方式或Columns方式，可指定多个Column
- 可指定写入Kafka时需要过滤的Columns

### 高级功能
- 集群高可用：http服务无状态横向扩容，worker服务可在线迁移任务
- 任务分配负载均衡：根据每个进程的任务数（后续可支持服务器资源）
- 数据断点续传：每传输一定量的binlog events，会记录当前的GTID，后续在此基础上继续传输
- 服务graceful shutdown：通过先停止抽取线程，再停止回放线程，并完成任务状态记录，之后再关闭服务。


## 横向对比
### dts vs Canal vs Maxwell
- Canal1.1.\*版本之前不支持数据到kafka.只能通过手动开发完成
- Maxwell只支持json格式，而canal的格式比较自由
- Maxwell部署非常方便，服务端客户端一体化。Canal则有adapter,serer需要分开部署。Canal相对更丰满一些
- Canal支持HA，Maxwell支持断点还原
- Maxwell支持bootstrap，对于数据初始化来说，Canal还不支持
- Maxwell支持全量增量同步到kafka.Canal全量暂时只能通过接口形式调用
- Canal依靠binlog的位点记录位置，Maxwell自己记录了position

|    | dts | Canal | Maxwell |
|  ----  | ----  | ---- | ---- |
| 公司 | 自研 | 阿里 | zendesk |
| 开发语言 | Go | Java | Java |
| 社区活跃度 | 暂无社区 | 活跃 | 活跃 |
| 高可用（HA） | 支持 | 支持 | 支持 |
| 数据去向（Sink） | Kafka/MySQL | Kafka等 | Kafka/Redis等 |
| 分区 | 支持 | 支持 | 支持 |
| 分区模式-库/表 | 支持 | 不支持 | 支持 |
| 数据格式 | JSON| 任意格式 | JSON |

### 自研dts vs 阿里云dts

|    | 自研dts | 阿里云dts |
|  ----  | :----:  | :----: |
| 多表到单表同步 | ✓ | × |
| 全量+增量数据同步 | ✓ | ✓ |
| 增量数据同步 | ✓ | ✓|
| 多库数据库同步 | ✓ | ✓ |
| 多表数据同步 | ✓ | ✓ |
| 双向同步 | ✓ | ✓ |
| fixed格式的binlog | ✓ | × |
| 基于GTID的binlog events | ✓ | ✓ |
| 基于file pos的binlog events | × | ✓ |
| 普通ddl | ✓ | ✓ |
| gh-ost在线ddl | ✓ | ✓ |
| 基于column的filter，支持数字类型 | ✓ | ✓ |
| 基于column的filter，支持UTF-8字符串类型 | ✓ | ✓ |

## Roadmap

- [ ] DTS + dbproxy实现在线扩容
- [ ] 优化update，只更新变化字段
- [ ] 接入日志中心
- [ ] AP场景支持全量+增量同步

## 沟通交流
### 服务群
> 对已接入dts的业务提供在线技术支持服务
