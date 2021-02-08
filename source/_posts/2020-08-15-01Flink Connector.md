---
layout:     post
title:     01 Flink Connector
date:       2020-08-15
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    -  Flink 1.12
---
 

与 Spark 类似，Flink 作为分布式计算框架，从外接数据引擎获取数据，计算完成后，并把计算结果输出到外部数据引擎或者触发某些操作。Flink 使用 Connector 来描述外部数据引擎，使用 Source 来定义如何从 Connector 中获取数据，使用 Sink 来定义如何向 Collector 写入数据或者触发某些操作, e.g. 客户端 Feed 实时推荐、告警短信等。  
Flink 预定义了一些 Source 和 Sink，分为五类:  

1. 基于内存数据结构
2. 基于文件
3. 基于 Socket
4. 自定义 Connector  
5. 异步 IO

<!-- more -->

![Flink Connector](/img/pictures/flink/flink_connector.png)  

## 基于内存数据结构 

可以直接基于内存中的数据结构：不定长数组(T*)、序列(Seq[T])、迭代器(Iterator[T]) 使用 StreamExecutionEnvironment 相应的接口来构建 Source；输出数据可以直接通过 DataStream#print 或 DataStream#printToErr 来写出到标准输出 / 错误流。  

## 基于文件

从本地 / 远程文件和目录中读取文件内容，可以使用 StreamExecutionEnvironment#readFile 或 readFileStream 相关接口来构建 Source，并支持各类 FileInputFormat；输出数据可以通过 DataStream#writeAsText 或 writeAsCsv 来写入到文本文件或 CSV 文件。  

从文件读取数据时，所有接口最终调用的接口定义如下，需要注意不同场景下的 exactly once 语义： FileProcessingMode 分两种，PROCESS_ONCE 表示只处理目录 / 文件中已有的内容，处理完即退出不再监控文件后续的变化；PROCESS_CONTINUOUSLY 表示周期性(interval 参数)地扫描目录 / 文件以处理新的内容，对处理过的文件做修改会导致该文件二次处理。   

```
def readFile[T: TypeInformation](
                                    inputFormat: FileInputFormat[T],
                                    filePath: String,
                                    watchType: FileProcessingMode,
                                    interval: Long,
                                    filter: FilePathFilter): DataStream[T]
```

向文件写入数据时，所有接口最终调用的接口定义如下，写入 TextFile / CsvFile 使用的分别是 TextOutputFormat / ScalaCsvOutputFormat。OutputFormat 可以设置 WriteMode：OVERWRITE 表示 删除目录 / 文件后再创建新的目录 / 文件；NO_OVERWRITE 表示在目录 / 文件已存在时不覆盖，最终会抛出 IOException。  

```
@deprecated
def writeUsingOutputFormat(format: OutputFormat[T]): DataStreamSink[T]
```


## 基于 Socket
可以从 Socket(host,port)中接收文本数据；也可以将结果数据写入到 Socket     
 
## 自定义 Connector 
StreamExecutionEnvironment#createInput(InputFormat) 和 DataStream#writeUsingOutputFormat(OutputFormat) 是创建 DataSource 和 Sink 的通用方法，例如使用 AvroInput(Output)Format 连接 Avro 文件、使用 PrintingOutputFormat 实现 DataStream#print、使用 HBaseOutputFormat 写入到 HBase 等。  
apache-flink/flink-connectors 模块内置包括 Kafka、Jdbc 和 ElasticSearch 在内的最常用 Connector，Apache Bahir 工程也提供了写入 Flume、Redis 等数据源的Connector。内置的 Connector 虽然在 Flink 的源代码中，但是由于版本问题(kafka-0.10 / kafka-0.11,es-5 / es-6 / es-7)没有打包到二进制发布包中，因此需要应用提供相关依赖；引用 Apache Bahir 中的 Connector 时也需要引入相关依赖。     

![Flink Source Connectors](/img/pictures/flink/flink_src_connector.png)

## 异步 IO
使用 Connector 并非 Flink 接入 / 输出数据的唯一手段，另一种不太常见的模式是：在 Flink 函数中调用第三方服务(e.g. webservice、邮件服务器)作为 Source / Sink。Fink 为这种场景提供了异步 IO，以减少阻塞等待延时提高吞吐量。     


## 参考 

1. https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/connectors/  
2. https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/operators/asyncio.html   
