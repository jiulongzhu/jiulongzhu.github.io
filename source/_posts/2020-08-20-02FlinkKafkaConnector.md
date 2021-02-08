---
layout:     post
title:     02 FlinkKafkaConnector
date:       2020-08-20
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    -  Flink 1.12
---

基于 Flink 1.12-SNAPSHOT 源代码和 Flink-1.11 官方文档   

Flink 提供了 Flink Kafka Connector 读取 / 写入 Kafka，并可以保证"exactly-once"语义。

<!-- more -->

## Dependency
Kafka 不同版本之间的通信协议是不同的，因此 Flink 提供了多版本的 Flink-Kafka-Connector。虽然 Flink 源代码中包含多版本的 Flink-Kafka-Connector，但发布安装包中不包含，因此应用需要自带Flink-Kafka-Connector 依赖。

artifactId 中 011 / 010指 kafka 版本，2.11 指 scala 版本。

```
<!-- for kafka-0.11 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka-011_2.11</artifactId>
    <version>1.11.0</version>
</dependency>
```

```
<!-- for kafka-0.10; FlinkKafkaConnector010 不能在写入 kafka 时保证 exactly-once 语义-->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka-010_2.11</artifactId>
    <version>1.11.0</version>
</dependency>
```

## Kafka Consumer  

FlinkKafkaConsumer 提供了读取 Kafka 一个或多个 Topic 数据的入口，FlinkKafkaConsumer011 对应 flink-connector-kafka-011、FlinkKafkaConsumer010 对应 010....  

必须提供的信息：

1. topic 或者 topic 列表
2. 序列化 / 序列化 schema，以将 Kafka 二进制数据转换为内存中的对象
3. consumer 信息：broker (逗号分隔)、consumer group id

```
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")
stream = env
    .addSource(new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), properties))
```

### 序列化 / 反序列化 schema

Flink 提供了一下 序列化 / 反序列化 schema：

1. TypeInformationSerializationSchema / TypeInformationKeyValueSerializationSchema，前者用于把记录序列化或反序列化，后者用于把记录的 key / value 分别序列化或反序列化。这两种模式应用于 Kafka 数据是被 Flink 写入且读取时，是通用的序列化反序列化 schema 的高性能替代方式    
2. Json(De)serializationSchema / JSONKeyValue(De)serializationSchema，使用 Jackson 将数据以 ObjectNode 和 Json 之间互转。    
3. Avro(De)serializationSchema，使用静态的 AVRO 模式定义来 序列化 / 反序列化 数据，依赖是 org.apache.flink:flink-avro；并提供了另外一种变式，可以在 Kafka 的 Registry Schema 中读取数据写入时的模式，依赖是 org.apache.flink:flink-avro-confluent-registry。 AVRO 中最好不包含嵌套结构。    
4. 其他常用：SimpleStringSchema 等  
当遇到任何原因导致的反序列化失败时，反序列化返回 null 值。这将触发 Flink 的容错，最终会陷入 反序列化失败->容错重试->反序列化失败 的循环。  

### 定制开始消费位置

Flink Kafka Connector 提供了四种方式 允许定制在 topic partition 开始消费的位置：   

1. setStartFromGroupOffsets(默认方式)：从 consumer group 提交给 zookeeper 的 offset 处开始读取，如果 partition 当前 offset 区间不包含此 offset，那么 auto.offset.reset 将生效。   
2. setStartFromEarliest / setStartFromLatest：从 partition 的最老 / 最新 数据处开始读取。此模式将忽略 consumer group 提交给 zookeeper 的 offset。   
3. setStartFromTimestamp：每条记录发送到 broker 或 broker 确认记录时为数据打上的时间戳，使用此方式时将从 时间戳大于等于指定时间戳的记录处开始消费，如果 partition 中最新数据的时间戳低于此值，那么将从最新的数据处开始消费(存疑，等待还是消费)。此模式将忽略 consumer group 提交给 zookeeper 的 offset。    
4. setStartFromSpecificOffsets：可以指定从每个 partition 的 offset 处开始消费。如果在 topic 的所有 partition 中存在未指定 offset 的 partition，那么为此 partition 将回退(fallback)到 setStartFromGroupOffsets 模式从 zookeeper offset 处开始读取。  

```
val env = StreamExecutionEnvironment.getExecutionEnvironment()
 
val myConsumer = new FlinkKafkaConsumer[String](...)
myConsumer.setStartFromEarliest()      // start from the earliest record possible
myConsumer.setStartFromLatest()        // start from the latest record
myConsumer.setStartFromTimestamp(...)  // start from specified epoch timestamp (milliseconds)
myConsumer.setStartFromGroupOffsets()  // the default behaviour
 
val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()  // start from specific offset
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L)
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L)
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L)
myConsumer.setStartFromSpecificOffsets(specificStartOffsets)
val stream = env.addSource(myConsumer)

```

>
>当作业从 Checkpoint 或 SavePoint 中恢复时，这些消费位置的定制方法不会影响开始消费位置。  
>在恢复时，每个 topic partition offset 取值于存储在 SavePoint 或 CheckPoint 中的 offset。   


### Consumer 容错  

1. 当启用了 checkpoint 时，Flink Kafka Consumer 会从 topic 中消费记录并周期性地 checkpoint kafka offset 和其他操作的状态。当 Flink 应用失败时 会从最新的 checkpoint 中恢复并从  checkpoint 存储的 topic offset 处开始消费数据。  
2. 当禁用了 checkpoint 时，Flink Kafka Consumer 会周期性地提交 offset 到 zookeeper。   

### topic / partition 自动发现  

在生产环境中，应用消费的 topic 数量或者 topic 的 partition 数量可能是随 需求 / 负载扩容 等变化的。Flink 提供了机制以满足 topic 和 partition 自动发现。  

partition 自动发现：默认情况是分区自动发现是禁用的，设置 flink.partition-discovery.interval-millis 非负即可启用，所有自动发现的分区都会从分区最开始处消费。   
topic 自动发现：同分区自动发现，设置 flink.partition-discovery.interval-millis 非负且设置 topic 为正则表达式即可启用。  
FlinkKafkaConsumer 内部会启动一个独立的线程定期去 Kafka 获取最新的 meta 信息，并调整作业。  

```
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")
properties.set("flink.partition-discovery.interval-millis","600000")
 
val myConsumer = new FlinkKafkaConsumer[String](
  java.util.regex.Pattern.compile("test-topic-[0-9]"),
  new SimpleStringSchema,
  properties)

```

可以持续自动发现名称满足 以"test-topic"开头以 [0-9] 任一数字结尾的 topic。如果仅设置正则表达式而不设置 flink.partition-discovery.interval-millis，则只在应用启动时发现一次，不能持续自动发现。如果仅设置 flink.partition-discovery.interval-millis 不设置正则表达式，则只对分区自动发现。     

### Consumer 提交 Offset 回 Kafka 配置 

Flink Kafka Consumer 允许配置 Consumer 提交 Offset 回 Kafka 的方式，但是并不是为了使用 zookeeper offset 保证容错，而是为了展示消费进度以便于监控和滞后调整。  
Consumer 提交 Offset  回 Kafka 的方式取决于作业是否启用了 checkpoint。    

1. 启用 checkpoint：当 checkpoint 完成时，Flink Kafka Consumer 将提交存储在 checkpoint 中的 offset，这可以确保 checkpoint offset 和 zookeeper offset 一致。自动提交间隔取决于 checkpoint 间隔，且延时较大。用户可以调用 setCommitOffsetsOnCheckpoints(false) 来禁止提交到 zookeeper，默认情况下为 true。启用 checkpoint 时将忽略 enable.auto.commit / auto.commit.interval.ms 配置。  
2. 禁用 checkpoint：Flink Kafka Consumer 使用内部 Kafka Client 来周期性提交 offset。因此可以通过配置来 enable.auto.commit / auto.commit.interval.ms 来 启用 / 禁用 / 订制 自动提交 offset 到 zookeeper。 

### 水印线(WaterMark)  

一般称作水印，但是称作水印线会更贴切一些。  
很多场景下，记录的时间戳嵌入在记录本身或者 ConsumerRecord 的元数据中，此外 consumer 可能需要根据记录的时间戳 兼容数据乱序和触发一些操作。Flink Kafka Consumer 允许指定一个水印线策略。  


```
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")
val myConsumer =
    new FlinkKafkaConsumer("topic", new SimpleStringSchema(), properties);
myConsumer.assignTimestampsAndWatermarks(
    WatermarkStrategy.
        .forBoundedOutOfOrderness(Duration.ofSeconds(20)))
val stream = env.addSource(myConsumer)

```


WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)) 一般用于为无序数据创建水印策略时设置数据无序程度的上限。假设无序程度的上限为 B，那么当当前数据中时间戳为 T 时，Flink 认为不会再出现比 (T-B)更早的数据，即 如 B=20s，T=2020-08-10 10：00：20，那么认为不会再有早于 2020-08-10 10：00：00 的数据。 如果有早于 T-B 的数据 那么
会为每一条 早于 T-B 的数据触发独立的行为，e.g. window / 数据更新。过多早于 T-B 的数据会导致性能问题和数据一致性问题。  
因此无序程度上限 B 的设置十分重要，必须深度了解业务和数据情况才能设置。   

![WaterMark 和 Window](/img/pictures/flink/flink_watermark.png)   


## Kafka Producer

Flink 提供了写入数据流到 Kafka 一个或多个 Topic 的功能，FlinkKafkaProducer011 对应 kafka-0.11，FlinkKafkaProducer010 对应 kafka-010...  
必须提供的信息：  

1. 一个写入数据时默认的 topic 名称  
2. 序列化方式，将内存中的数据结构序列化成 Kafka 二进制数据  
3. Kafka Client 配置，bootstrap.servers 是必须的(逗号分隔)  
4. 一个容错语义，一般是 exactly-once   


```
val stream: DataStream[String] = ...
Properties properties = new Properties
properties.setProperty("bootstrap.servers", "localhost:9092")
val myProducer = new FlinkKafkaProducer[String](
        "my-topic",                  // default target topic
        new SimpleStringSchema(),    // serialization schema
        properties,                  // producer config
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE) // fault-tolerance
stream.addSink(myProducer)

```

### Producer 容错

当启用 checkpoint 时，FlinkKafkaProducer011 可以提供 exactly-once 语义，FlinkKafkaProducer010 可以提供 at-least-once 语义 不能提供 exactly-once 语义。    
可以指定的语义：  

1. Semantic.NONE: Flink 不保证任何事，记录可能会丢失也可能会重复   
2. Semantic.AT\_LEAST\_ONCE(默认): Flink 保证记录不会丢失，但是可能会重复  
3. Semantic.EXACTLY\_ONCE: 使用 Kafka 事务来保证 exactly-once 语义。当使用事务向 Kafka 写数据时，需要对相关 topic 的 consumer 设置"隔离级别(isolation.level)"。隔离级别分两种 read\_committed 或 read\_uncommitted(默认)。   

>  
>注意事项:
>      
>1. Semantic.EXACTLY\_ONCE mode relies on the ability to commit transactions that were started before taking a checkpoint, after recovering from the said checkpoint. If the time between Flink application crash and completed restart is larger than Kafka’s transaction timeout there will be data loss (Kafka will automatically abort transactions that exceeded timeout time).
>exactly-once 语义依赖于 提交 “开始于 checkpoint 之前” 和 “从 checkpoint 恢复之后”的事务的能力。如果 Flink 应用从崩溃到完全恢复所用的时间大于 Kafka 事务超时时间，那么数据会丢失(Kafka 会自动放弃超时事务)。 
>2. 由于“译不准"，此处似乎也可以理解为 "exactly-once 语义依赖于提交特殊事务的能力，特殊事务开始于 checkpoint 之前，终止于从 checkpoint 恢复之后"。事务开启，状态尚未存储而应用挂掉，恢复后事务超时被 Broker 放弃导致了数据丢失，无法保证"exactly-once"语义。  


FlinkKafkaProducer 要求"exactly-once"语义时，topic 的所有 consumer 都必须设置隔离级别，默认为 read\_uncommitted。    

>  
>关于 read_committed 模式，在 KafkaConsumer 启用了 read\_committed 模式时，任何 未完成 / 未终止 的事务将会阻塞对该事务之后的所有事务的读取。  例如:     
>
>1. producer 开启 transaction1 并写入了一些记录    
>2. producer 开启了 transaction2 并写入了其他记录    
>3. producer commit transaction2  
>即使 transaction2 已经被 commit，其记录对于所有的 consumer 都不可见，直到 transaction1 被提交或终止。  这种模式有两个含义：  
>1. 在 Flink application 正常运行期间，输出到 topic 记录的可见性会有一定延迟，这等于已完成 checkpoint 的平均时间。   
>2. 当 Flink application 失败，将阻塞其写入的 topic 的所有 consumer，直到应用重启或者 Kafka transaction 超时。在有多个 producer / consumer 同时操作同一个 topic 时"exacty-once"造成的阻塞风险需要评估。      
 
### 分区方案  

使用 FlinkKafkaProducer 向 Kafka 写数据时，如果不指定分区器 Partitioner，默认使用 FlinkFixedPartitioner。该 Partitioner 分区的方式是将 Task 所在的实例 Id 按照 topic partition 总数取余将 sink 映射到单个的 topic partition 上，即：partitions[parallelInstanceId % partitions.length]。如果 topic partition 总数为 5，sink 并发度为 2，那么最终只有两个 topic partition 有数据；如果 sink 并发度为 6，那么 1 号 partition 有两个 sink 在写，会导致 Broker 负载均衡问题。  
如果指定 Partitioner 为 null，会使用 kafka producer 默认的分区方式，sink 可能会轮询写所有 partition。topic partition 的数据比较均衡，但是会相对保持更多的网络连接。     
因此使用 FlinkFixedPartitioner 并配置 sink 并发度为 topic partition 数量的整数倍较合理。    

## Kerberos 认证  
Flink 为连接到开启了 Kerberos 认证的 Kafka 集群 通过 FlinkKafkaConnector 提供了支持。只需要在 flink-conf.yaml 中简单配置即可启用 kerberos    

1. 配置以下配置项  
　　　- security.kerberos.login.use-ticket-cache：默认情况下为 true，Flink 尝试使用 kinit 管理的 ticket cache 中的 kerberos 证书(credentials)，但是当 Flink Job 被部署到 Yarn 和 Mesos 时，这个配置是不生效的，因为 Yarn 和 Mesos 不支持使用 ticket cache 中的证书认证      
　　　- security.kerberos.login.keytab and security.kerberos.login.principal：使用 kerberos keytab 来认证     
2. 在 security.kerberos.login.contexts 配置后追加 KafkaClient：此配置告诉 Flink 提供已配置的 kerberos 证书给 Kafka Login Context 以进行 Kafka 认证    


一旦基于 Kerberos 认证的 Flink 安全机制启用后，可以在 FlinkKafkaProducer / FlinkKafkaConsumer 中添加两个配置以传给内部的 KafkaClient 来进行 Kafka 认证。    

1. 设置 security.protocol 为 SASL\_PLAINTEXT(默认 NONE): 此协议用于和 Broker 通信。使用 standalone 部署 Flink 时可以设置 security.protocol 为 SASL\_SSL。
2. 设置 sasl.kerberos.service.name 为 kafka(默认 kafka)：这个值应该和 Broker 配置的 sasl.kerberos.service.name 值一致。客户端和服务端的服务名称不一致将导致认证失败


## 参考  

1. https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/connectors/kafka.html#kafka-consumer  
2. https://blog.csdn.net/lmalds/article/details/52704170  

 
