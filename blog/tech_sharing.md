# 什么是DTS
数据传输服务（Data Transmission Service，简称DTS）是一款面向MySQL用户的数据传输服务，提供基于binlog的增量和全量 + 增量的数据同步服务。该服务由Go语言开发。
自研DTS沿用了阿里云的DTS名称，旨在提供一个技术上更可控、更定制化、成本更优的MySQL基础服务。
后续，类似阿里云DTS，自研DTS也会服务于更多的数据库场景，如redis、MongoDB。

# 为什么需要DTS
当前我们遇到哪些问题？

## MySQL数据库跨库同步
![跨库同步dts.png](https://github.com/nicholaskh/opendts/blob/master/image/%E8%B7%A8%E5%BA%93%E5%90%8C%E6%AD%A5dts.png)

如上图所示，业务开发过程中，我们偶尔需要同步一个MySQL Database的数据到另一个Database里。同步过程中，可能会修改库名、表名、字段名，以及做一些过滤操作。
比如user库的user表，在微服务架构中，可能需要和每个微服务的MySQL都在同一个实例里存储一份，以此来方便做业务上的join操作。通常的操作我们会在user服务里把写入双写到kafka里，其它服务订阅kafka来更新各自的user库。而如果有了DTS，操作就方便多了，我们只需要建一个同步链路即可，其它的就交给DTS处理了。

## MySQL数据库扩容
![DB扩容.png](https://github.com/nicholaskh/opendts/blob/master/image/DB%E6%89%A9%E5%AE%B9.png)

如上图所示，在做分库分表的过程中，我们有时候会希望对现有的架构进行扩容，如3个库扩展到6个库，这个时候会涉及到数据的迁移，以及存量数据迁移后无缝开启增量数据同步。
数据迁移过程中，我们往往会迁移部分命中分片逻辑的数据，如下图所示，新的DB5是id%6=4（6个分片，余数从0开始），而DB6是id%6=5。

![DTS扩容.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627540487/DTS%E6%89%A9%E5%AE%B9.png)

迁移过程中，我们会使用DTS的filter功能，只迁移id % 6 = x的数据。
DTS中过滤器由SQL语句表示，以上过滤器写为：
```SQL
SELECT * FROM user WHERE id % 6 = 4;
```

随后，我们在迁移过程中可以开启“全量+增量”模式，在完成存量数据迁移后，会自动进行增量数据的同步，直到满足一定的阈值。
这个过程中，如果同时接入了dbproxy，那么生活会非常美好，我们只需要等候整个任务完成就行，中间的操作，DTS会自动与dbproxy进行通信，修改其分片路由转发规则。

## OLAP场景，MySQL数据同步到Kafka
![AP DTS.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627541190/AP%20DTS.png)

如上图所示，在有线下数据分析的场景下，我们往往希望把MySQL里的数据实时同步一份到线下AP类数据库中，如Hive表、Clickhouse等，来支持尽可能实时的数据分析输出。以往，在没有DTS的时候，我们往往需要业务方手动双写一份到kafka里，并且各个业务方在写入kafka的时候需要确保写入的数据格式的一致性。
而一旦有了DTS，事情将变得非常简单。我们只需要针对数据表创建一个DTS任务，MySQL数据就会“实时”地同步到kafka里。
我们目前支持JSON格式的数据序列化，举例如下所示：
```JSON
{"table":"xes_arris_enrollment_course_stu_1_0","database":"xes_arris_enrollment0","ts":1626959641,"type":"insert","data":{"class_id":0,"cou_stu_status":0,"counselor_id":0,"course_id":0,"created":0,"id":3,"stu_id":0,"updated":0}}
{"table":"xes_arris_enrollment_course_stu_1_0","database":"xes_arris_enrollment0","ts":1626959656,"type":"delete","data":{"class_id":0,"cou_stu_status":0,"counselor_id":0,"course_id":0,"created":0,"id":3,"stu_id":0,"updated":0}}
{"table":"xes_arris_enrollment_teacher_course_1_0","database":"xes_arris_enrollment0","ts":1626959681,"type":"insert","data":{"course_id":0,"course_name":"","course_status":1,"created":0,"difficulty_id":0,"difficulty_name":"","final_plan_etime":0,"first_plan_stime":0,"grade_ids":"","grade_names":"","id":1,"outline_ids":"","outline_names":"","relation_status":1,"school_schedule":"","school_schedule_name":"","schooltime_name":"","subject_ids":"","subject_names":"","teacher_id":0,"term_ids":"","term_names":"","tutor_ids":"","tutor_names":"","type_1_id":0,"type_1_name":"","type_2_id":0,"updated":0,"version_id":0,"version_name":"","year_id":0}}
```

## 双活场景，主从机房间通过DTS做数据实时同步

![双活dts.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627542062/%E5%8F%8C%E6%B4%BBdts.png)

双活场景中，DTS一般部署在对端目标机房，我们通过DTS抽取MySQL binlog来做实时的数据同步。在毫秒级延迟的专线环境下，一般的同步延迟（sbm）在一秒以下。
通过DTS的数据抽取与重放服务的分开部署，我们还能进一步减小专线的吞吐量。即将数据抽取服务部署在源机房，抽取过程中过滤掉不需要的数据，只传输需要的数据到目标机房。
在跨机房传输过程中，我们也能使用专线网关来做数据的压缩和流量平滑操作。

## 为什么选择自研
以上是DTS的几个常用case。除了以上几个场景，还有很多场景我们也需要DTS的加持，如：
- 缓存数据实时同步到MySQL
- 异构数据同步：如MySQL数据到MongoDB同步
- 数据实时备份
- 边缘机房数据实时汇总到数据中心


既然阿里云已经有DTS服务，且也有不少开源的数据同步产品，我们为什么还需要自研一套DTS呢？
主要有以下5个原因：
- 同时支持TP + AP场景
- 方便部署管理，运维成本低
- 更强的技术掌控，意味着可用性不只是纸上谈兵，且提供更高的稳定性
- 高定制化的需求支持，更快的响应
- 成本优化，比阿里云DTS便宜1个数量级

# 实现细节
## 系统架构
![DTS architecture.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627542860/DTS%20architecture.png)

如图是DTS的系统架构，我们看到大致的分层和模块化：
分层：
- Http Server：一个http接口服务，提供外界对DTS任务的增删查改，及任务的调度
- task层：主要是任务的提交、启动、全量、增量等操作
- stream层：封装了2个流式数据传输，基于binlog的增量流式操作和基于全量的snapshot全量流式操作
- 基础库：MySQL协议解析基础库、sqlparser、mysql schema相关调用

## 跨机房部署架构

![datastreaming dts.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627542808/data-streaming%20dts.png)

如图所示，DTS一般和目标数据集群部署在一起。
在基于DBAgent抽取数据的基础上，也可以将抽取逻辑放在DBAgent里，将数据抽取、过滤放到源机房，重放逻辑放到目标机房里。

## 软件部署架构

![replicator部署.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627543432/replicator%E9%83%A8%E7%BD%B2.png)

如图所示，DTS在部署上会依赖以下几个组件：
- Etcd：用来做服务注册及一些实例源信息存储
- MySQL：用来存储任务的一些源信息，包括当前已同步的GTID

DTS内部大概分为两个服务：
- HTTP服务：调度层，类似一个代理
- worker服务：实际的跑DTS任务的Goroutine协程

## 全量 + 增量同步原理

![replicator全量增量复制.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627543642/replicator%E5%85%A8%E9%87%8F%2B%E5%A2%9E%E9%87%8F%E5%A4%8D%E5%88%B6.png)

## 基于GTID的binlog抽取

基于GTID dump binlog的命令格式如图所示：
![gtid binlog format.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627543854/gtid%20binlog%20format.png)

其中的data字段：
![binlog data format.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627543774/binlog%20data%20format.png)

## Binlog Event structure - v4

MySQL传输过来的binlog二进制流如图
![v4 event structure.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627544454/v4%20event%20structure.png)

# DTS当前功能集
## 基础功能
- 支持通过SQL语句指定数据抽取的filter过滤器
- 支持数字型字段，字符串字段仅支持UTF-8编码
- 支持gh-ost online DDL
- 单任务同时抽取单个MySQL实例的多个库多个表
- 普通DDL

## OLAP功能
- 完全兼容Maxwell格式
- MySQL到kafka的增量数据同步
- 指定kafka topic，table形式或database形式
- 指定kafka分片方式，table方式或Columns方式，可指定多个Column
- 可指定写入kafka时需要过滤的Columns

## OLTP功能
- MySQL到MySQL数据实时增量同步
- MySQL到MySQL数据全量+增量同步
- MySQL分片平滑扩容（需要dbproxy配合）（开发中）
- 基于GTID的双向数据同步

## 高级功能
- 集群高可用：http服务无状态横向扩容，worker服务可在线迁移任务
- 任务分配负载均衡：根据每个进程的任务数（后续可支持服务器资源）
- 数据断点续传：每传输一定量的binlog events，会记录当前的GTID，后续在此基础上继续传输
- 服务graceful shutdown：通过先停止抽取协程，再停止重放协程，并完成任务状态记录，之后再关闭服务

# 用户接入

![用户接入dts流程.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627545195/%E7%94%A8%E6%88%B7%E6%8E%A5%E5%85%A5dts%E6%B5%81%E7%A8%8B.png)

后续我们会把接入流程一站式集成到zeus 2.0上。

# 业界对比
## AP场景
![dts横向对比.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627545327/dts%E6%A8%AA%E5%90%91%E5%AF%B9%E6%AF%94.png)

## TP场景
![dts vs阿里云dts.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627545328/dts%20vs%E9%98%BF%E9%87%8C%E4%BA%91dts.png)

# 监控
## grafana + prometheus
![dts监控2.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627545455/dts%E7%9B%91%E6%8E%A72.png)

![dts监控1.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627545457/dts%E7%9B%91%E6%8E%A71.png)

## 日志监控（TBD）

# 压测
![replicator_test_rows_qps.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627547157/replicator_test_rows_qps.png)

![replicator_test_kafka_qps.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627545710/replicator_test_kafka_qps.png)

## 火焰图
### inuse_space
![replicator_inuse_space.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627547261/replicator_inuse_space.png)

### on_cpu
![replicator_cpu.png](http://ttc-tal.oss-cn-beijing.aliyuncs.com/1627545568/replicator_cpu.png)

# Roadmap
- DTS + dbproxy实现在线自动扩容
- 优化update，只更新变化字段
- 接入日志中心
- AP场景支持全量+增量模式
- 支持其它数据库，如：redis、MongoDB

# 参考链接
- com-binlog-dump-gtid：https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
- 按照GTID同步数据说明：https://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html
- binlog event类型：http://dev.mysql.com/doc/internals/en/binlog-event-type.html
- Maxwell：https://github.com/zendesk/maxwell
- Canal：https://github.com/alibaba/canal
- 阿里云DTS：https://help.aliyun.com/product/26590.html
- DTS未来云文档：http://cloud.tal.com/docs/dts/
- zeus 2.0：http://cloud.tal.com/zeus-fe/workBench/index
