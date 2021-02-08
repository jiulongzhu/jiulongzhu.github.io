---
layout:     post
title:     Apache Spark 3.0 Preview 新特性和改动 
subtitle:   Spark 3.0 预览版
date:       2019-12-27
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - Spark 3.0 
---


## 概述  

Apache Spark 3.0 Preview(预览版)发布于 2019-11-23。就 API 和功能而言预览版不是稳定版本，本文将列出预览版的重要新特性和改动。  

## 新特性   

### Adaptive Execution Of SparkSQL(适应性执行,AE)  

适应性执行使用运行时的统计数据进行动态优化。通过使物理计划分阶段(不同于DAG的stage)并在运行期可修改，主要的优化点有:  

1. BroadcastHashJoin(BHJ): 基础表的大小/行数来自于表 Statistics，中间操作产生的数据集大小/行数靠估计，基础表统计数据可能是错误的，中间数据集统计数据可能失真(e.g. 数据倾斜)。导致运行前制定的物理计划不适合运行时，例如运行前估计的中间数据集大小为 1GB 而选择 SortMergeJoin 但运行时发现中间数据集大小为 10MB 可以修改 join 策略为 BHJ。  
2. 分区数确定问题: 当分区输入数据量很小时(数据空洞,数据倾斜)，可以使一个task 处理多个连续分区的数据量(总量不超过指定值, e.g. 64MB)。      
3. 数据倾斜问题: 当分区输入数据量大于所有分区输入量中位数的指定倍数时，认为该分区数据倾斜，对此分区使用多个线程并行处理。    

<!-- more -->
注:  
1. [SPARK-9850](https://issues.apache.org/jira/browse/SPARK-9850)  
2. [Intel Spark AE](https://github.com/Intel-bigdata/spark-adaptive.git)  

### Dynamic Partition Pruning(动态分区裁剪,DPP)  

Spark3.0引入了动态分区裁剪特性，这是 SQL 分析工作的一项重大性能改进，可以与 BI 工具集成得更好。动态分区裁剪的原理是将应用在维度表上的过滤器集合直接应用到事实表上，因此可以跳过扫描非必需的分区，减少事实表扫描的数据量。在逻辑计划和物理计划上均可以实施 DPP 策略，DPP 加速了大部分 TCPDS 查询并且在无需非规范化表的情况下很好的兼容星型模型。  

注:   
1.  非规范化是一种在规范化数据库之前应用以提升性能的策略。原理是通过数据冗余减少数据关联 牺牲数据的写性能以提升读性能。  
2.  [Dynamic Partition Prunning](https://databricks.com/session_eu19/dynamic-partition-pruning-in-apache-spark)    

### Enhanced Support for Deep Learning

加强了对深度学习的支持。在 Spark 3.0之前，Spark MLlib 并不专注于深度学习 没有提供图像/NLP 相关的深度学习算法。yahoo 的 TensorFlowOnSpark 等工程提供了在 Spark 上深度学习的可能，但是有很大的问题: 对 Spark 弹性计算兼容不足，单分区训练失败将在所有分区上再训练。Spark 3.0解决了这个问题，并兼容了Nvidia、AMD、Intel 等多类型 GPU    

### DataSourceV2 
DataSourceV1 对数据源的处理有一些劣势: 过于依赖 SQLContext/DataFrame,导致底层 API 依赖高层 API；不支持列式存储的批处理接口；不支持 SQLOnStreaming 等  
Spark 3.0对 DataSourceV2的优化:  

1. 改进谓词下推机制，可通过减少数据加载来加速查询  
2. 执行可插拔的 Catalog 插件
3. 支持列式存储批处理接口以提高性能  
4. 支持读二进制文件 Binary File DataSource,但不支持写二进制 DataFrame 到外部数据源      

注:  
[SPARK-15689](https://issues.apache.org/jira/browse/SPARK-15689)  

### Spark Delta Lake

Delta Lake 是一个引入了 ACID事务的开源存储层，保证了数据湖的可靠性、性能和生命周期管理。Delta Lake 可以插件式得实现或升级现有程序来事务性删除/修改大数据中的小数据。对流处理(e.g. 流式数据仓库)、数仓中拉链表等场景提供了新的方案。    

注:  
[Delta Lake](https://docs.databricks.com/delta/quick-start.html)

### Graph Feature(存疑)

Spark 3.0加入了SparkGraph 模块用于进行图处理，可以使用 Neo4J开发的 Cypher 语言操作图模型及图算法。Neo4J 图数据库是单节点的，不适用于大数据量，SparkGraph 如何解决这个问题的?(未知)。    

### Yarn  

Spark 3.0 可以在 Yarn 集群上自动发现GPU，并可以在 GPU 节点上调度任务(task)。   

### Kubernetes  

Spark 2 时代对Kubernetes 的支持相对不太成熟，在生产环境中相比 Yarn 来说更难使用且性能较差。Spark3.0 引入了新的 Shuffle Service On Kubernetes 且支持 GPU 的 Pod 级隔离。  

## 改动  

### 语言支持  

预览版及其后版本支持 Python3、Scala 2.12和 JDK11。Python2 将被弃用。  

### hadoop支持

支持 Hadoop 3，预览版默认为 Hadoop2.7.4。  

### deprecated  
删除了不高于 Spark2.2.0 的 deprecated API、参数和功能等。

## issues附录  
[issues](https://issues.apache.org/jira/sr/jira.issueviews:searchrequest-printable/temp/SearchRequest.html?jqlQuery=statusCategory+%3D+done+AND+project+%3D+12315420+AND+fixVersion+%3D+12339177+ORDER+BY+priority+DESC%2C+key+ASC&tempMax=1000)
