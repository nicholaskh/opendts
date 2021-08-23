# dts HTTP接口

## 通用参数
### 通用请求Header
> 每个请求必带

|Header|内容|说明|
|:----|:---|:-----|
|Authorization|Basic cmVwbGljYXRvcjpkdHNmb3IzLjE0MTU5MjY=|basic http校验，仅限测试环境|
|Content-Type|application/json|入参类型|

### 通用返回
|参数|类型|内容|说明|
|:----|:---|:-----|:-----|
|code|int|0\|其它整数|0表示成功，其它表示error code|
|message|string|错误信息|code非0时有效|
|ttl|int|忽略|忽略|
|data|json|具体返回信息||


## 接口列表

### 提交任务接口
#### URL
/v1/dispatch/submit

#### Method
POST

#### 请求参数

|参数名|类型|说明|
|:----    |:---|:----- |
|task|json|提交的任务,包含的各个参数见下面说明【只传task】|
|source|json|数据源的mysql连接【只传ip,端口】|
|kafka|json|- 写入的kafka<br>- flat_format: 表示内容是否打平，暂时可省略<br>- topic_mode命名方式：1:dbname_talblename, 0:dbname<br>- partition_mode：table:按表名分区, column:按列分区，需要填partition_columns, primary_key:按主键分区 <br>- producer_partition_by_fallback：如果按column分区，指定的column不存在的话，回退到哪种方式，可填table或primary_key <br>- exclude_columns：需要排除的column|
|filter|json|过滤的库表和字段值<br>dbname不支持正则<br>match为表，可写正则，发送http请求时需要注意转义字符<br>【filter 计算的sql-可不传】|
|rate_limit|json|rate为允许的最大QPS<br>capacity为桶的容量，一般与rate保持一致即可<br>非必传 单位qps|
|incr|bool|是否增量复制<br>kafka类型的业务写true就行，false为先复制存量数据|

#### 返回
|名称|类型|举例|说明|
|:----- |:----- |:----- |:----- |
|addr|string||任务分发后具体执行的机器http地址|
|id|int|1|新建的任务ID，自增主键|


#### 举例
```bash
curl -i -XPOST -H 'content-type:application/json' -H 'Authorization:Basic cmVwbGljYXRvcjpkdHNmb3IzLjE0MTU5MjY=' -d '{"task":{"source":{"host":"127.0.0.1","port":5306,"username":"xes_replicator","password":"IYQBXGil84d44ee92cLjLHankV2rVyCg+"},"kafka":{"addr":"127.0.0.1:9092","topic":"replicator_test","flat_format":false},"filters":[{"dbname":"oa_data_service","match":"/xes_class_students_\\d+/","filter":""},{"dbname":"oa_data_service","match":"check_delay","filter":""}],"incr":true}}' 'http://127.0.0.1:8080/v1/dispatch/submit'
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Kratos-Status-Code: 0
Kratos-Trace-Id:
Date: Thu, 08 Apr 2021 07:41:41 GMT
Content-Length: 48

{"code":0,"message":"0","ttl":1,"data":{"addr":"127.0.0.1:8080", "id":1}}
```

### 任务列表接口

#### URL
/v1/task/list

#### Method
GET

#### 请求参数
> 由于是GET请求，参数均在url query里

|参数名|类型|说明|
|:---- |:---|:----- |
|per_page|int|每页条数|
|page|int|第几页|

#### 返回
|名称|类型|说明|
|:----- |:----- |:----- |
|tasks|json|返回的task列表<br>具体参数见下面|
|id|int|Task ID|
|source_addr|string|mysql binlog数据源地址|
|dest_addr|string|写入的kafka地址|
|pos|string|当前拉取到的mysql binlog位置|
|transaction_time|string|最后一条binlog在mysql数据源的执行时间|
|state|string|任务当前状态|
|updated_at|string|任务更新时间|
|message|string|错误信息|
|service_addr|string|任务所在的机器地址|


#### 举例
```bash
curl -i -H 'content-type:application/json' -H 'Authorization:Basic cmVwbGljYXRvcjpkdHNmb3IzLjE0MTU5MjY=' 'http://127.0.0.1:8080/v1/task/list?per_page=5&page=1'
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Kratos-Status-Code: 0
Kratos-Trace-Id:
Date: Fri, 09 Apr 2021 04:03:40 GMT
Content-Length: 330

{"code":0,"message":"0","ttl":1,"data":{"tasks":[{"id":1,"source_addr":"127.0.0.1:5306","dest_addr":"127.0.0.1:9092","pos":"MySQL56/d48092bf-40ea-11e9-8663-b496913726e0:1-1202949000","updated_at":"2021-04-09 12:03:22","transaction_time":"2021-04-09 12:03:20","state":"Running","message":"","service_addr":"127.0.0.1:8080"}]}}
```

### 修改任务接口

#### URL
/v1/dispatch/alter/:id

#### Method
PUT

#### 请求参数
同“提交任务”，source字段不可改，incr字段不可改

#### 返回
通用返回体

#### 举例
```bash
curl -i -XPUT -H 'content-type:application/json' -H 'Authorization:Basic cmVwbGljYXRvcjpkdHNmb3IzLjE0MTU5MjY=' -d '{"task":{"source":{"host":"127.0.0.1","port":5306,"username":"xes_replicator","password":"IYQBXGil84d44ee92cLjLHankV2rVyCg+"},"kafka":{"addr":"127.0.0.1:9092","topic":"replicator_test","flat_format":false},"filters":[{"dbname":"oa_data_service","match":"/xes_class_students_\\d+/","filter":""},{"dbname":"oa_data_service","match":"check_delay","filter":""}],"incr":true}}' 'http://127.0.0.1:8080/v1/dispatch/alter/1'
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Kratos-Status-Code: 0
Kratos-Trace-Id:
Date: Fri, 09 Apr 2021 03:10:11 GMT
Content-Length: 42

{"code":0,"message":"0","ttl":1,"data":{}}
```

