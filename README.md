# Logstash Logservice Input Plugin

This is a plugin for [Logstash](https://github.com/elastic/logstash).

## Documentation
This plugin provides infrastructure to automatically consume logs from Aliyun Log Service .


### Usage:
This is an example of logstash config:
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
  query => "your SLS SPL"
  }
}

output {
  stdout {}
}
```

### Logstash Logservice Input Configuration Options
This plugin supports the following configuration options

|Configuration|Type|Required|Comments|
|:---:|:---:|:---:|:---|
|endpoint|string|true|Your project endpoint|
|access_id|string|true|Your access id|
|access_key|string|true|Your access key|
|project|string|true|Your project name|
|logstore|string|true|Your consumer_name name|
|consumer_group|string|true|Consumer group name|
|consumer_name|string|true|Consumer name,The consumer name in the same consumer group must not be repeated, otherwise undefined behavior will occur.|
|position|string|true|Position to consume. Options are `begin`, `end` and `yyyy-MM-dd HH:mm:ss`|
|checkpoint_second|number| false|Time to checkpoint,default is 30|
|include_meta|boolean| false|Whether the meta is included,default is true|
|consumer_name_with_ip|boolean| false|Whether the consumer name has ip,default is true,Must be set to true under distributed collaborative consumption|
|query|string| false | SLS SPL, refer: https://help.aliyun.com/zh/sls/user-guide/spl-overview  |



## Install the plugin

you can build the gem and install it using:

- Install the plugin from the Logstash home

```sh
logstash-plugin install logstash-input-sls
```

- Start Logstash

```bash
logstash -f logstash-sample.conf
```

## The performance test

### The test environment

- cpu : Intel(R) Xeon(R) Platinum 8163 CPU @ 2.50GHz,4 Core
- memory : 8GB 
- env : Linux

### Test result
| Data flow(MB/S) |CPU(%) | MEM(GB) |
| :---: | :---: | :---: |
|32|170.3|1.3|
|16|83.3|1.3|
|8|41.5|1.3|
|4|21.0|1.3|
|2|11.3|1.3|