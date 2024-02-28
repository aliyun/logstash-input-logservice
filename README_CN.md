# Logstash 日志服务 Input 插件

本插件作为[Logstash](https://github.com/elastic/logstash)的input插件，提供从日志服务拉取（消费）日志的功能。

### 功能特性
* 支持分布式协同消费：可配置多台服务器同时消费某一Logstore。
* 高性能：基于Java ConsumerGroup实现，单核消费速度可达20MB/s。
* 高可靠：消费进度保存到服务端，宕机恢复后会从上一次checkpoint处自动恢复。
* 自动负载均衡：根据消费者数量自动分配Shard，消费者增加/退出后会自动Rebalance。


### 使用方式:
以下示例为配置Logstash消费某一个Logstore并将日志打印到标准输出:
```
input {
  logservice{
  endpoint => "your project endpoint"
  access_id => "your access id"
  access_key => "your access key"
  project => "your project name"
  logstore => "your logstore name"
  consumer_group => "consumer group name"
  consumer_name => "consumer name"
  position => "end"
  checkpoint_second => 30
  include_meta => true
  consumer_name_with_ip => true
  }
}

output {
  stdout {}
}
```
分布式并发消费的配置：
````
例如某Logstore有10个shard，
每个shard数据流量1M/s，
每台机器处理的能力为3M/s，
可分配5台服务器，
每个服务器设置相同的consumer_group和consumer_name，consumer_name_with_ip字段设置为true。
这种情况每台服务器会分配到2个Shard，分别处理2M/s的数据。
````


### Logstash 日志服务 Input 配置参数
本插件提供以下配置参数

|参数名|参数类型|是否必填|备注|
|:---:|:---:|:---:|:---|
|endpoint|string|是|日志服务项目所在的endpoint，详情请参考[endpoint列表](https://help.aliyun.com/document_detail/29008.html)|
|access_id|string|是|阿里云Access Key ID，需要具备ConsumerGroup相关权限，详情请参考[consumer group](https://help.aliyun.com/document_detail/28998.html)|
|access_key|string|是|阿里云Access Key Secret，需要具备ConsumerGroup相关权限，详情请参考[consumer group](https://help.aliyun.com/document_detail/28998.html)|
|project|string|是|日志服务项目名|
|logstore|string|是|日志服务日志库名|
|consumer_group|string|是|消费组名|
|consumer_name|string|是|消费者名，同一个消费组内消费者名，必须不能重复，否则会出现未定义行为|
|position|string|是|消费位置，可选项为 `begin`（从日志库写入的第一条数据开始消费）、`end`（从当前时间点开始消费） 和 `yyyy-MM-dd HH:mm:ss`（从指定时间点开始消费）|
|checkpoint_second|number| 否|每隔几秒 checkpoint 一次，建议10-60秒，不能低于10秒，默认30秒|
|include_meta|boolean| 否|传入日志是否包含meta，Meta包括日志source、time、tag、topic，默认为 true|
|consumer_name_with_ip|boolean| 否|消费者名是否包含ip地址，默认为 true，分布式协同消费下必须设置为true|
|query|string| false | SLS SPL, refer: https://help.aliyun.com/zh/sls/user-guide/spl-overview  |


## 安装插件

- 安装 日志服务 input 插件

```sh
logstash-plugin install logstash-input-sls
```

- 启动 Logstash

```bash
logstash -f logstash.conf
```

## 性能基准测试

### 测试环境

- 处理器 : Intel(R) Xeon(R) Platinum 8163 CPU @ 2.50GHz,4 Core
- 内存 : 8GB 
- 环境 : Linux

### Logstash配置

```
input {
  logservice{
  endpoint => "cn-hangzhou.log.aliyuncs.com"
  access_id => "***"
  access_key => "***"
  project => "test-project"
  logstore => "logstore1"
  consumer_group => "consumer_group1"
  consumer_name => "consumer1"
  position => "end"
  checkpoint_second => 30
  include_meta => true
  consumer_name_with_ip => true
  }
}

output {
  file {
  path => "/dev/null"
  }
}
```

### 测试过程

- 使用Java Producer向logstore发送数据，分别达到每秒发送2MB、4MB、8MB、16MB、32MB数据。
- 每条日志约500字节，包括10个Key&Value对。
- 启动Logstash，消费logstore中的数据并确保消费延迟没有上涨（消费速度能够跟上生产的速度）。

### 测试结果
| 流量(MB/S) |处理器占用(%) | 内存占用(GB) |
| :---: | :---: | :---: |
|32|170.3|1.3|
|16|83.3|1.3|
|8|41.5|1.3|
|4|21.0|1.3|
|2|11.3|1.3|
