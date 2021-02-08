---
layout:     post
title:     Apache Spark TPC-DS 性能测试数据
subtitle:   TPCDS
date:       2020-02-14
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - Spark 2.3.0
    - Adaptive Execution 
---


## 关于 TPC-DS  
TPC(Transaction Process Performance Council, 事务处理性能委员会)是制定商务应用基准程序(Benchmark)的标准规范、性能和价格度量的非营利性组织。TPC-DS 是一个决策支持标准测试，它对决策支持系统的普适性建模，包括查询和数据修改维护。作为一个通用的决策支持标准测试，其提供了具有代表性的性能评估，测试结果包括单用户模式下的查询相应时间、多用户模式下的查询吞吐量、在受控复杂多用户决策支持的工作负载下给定硬件 操作系统 数据处理系统软件配置的数据维护性能。TPC-DS 决策支持标准测试目的是为行业用户提供客观的性能数据。TPC-DS 决策支持一些新兴技术，例如 Hadoop、Spark等大数据解决方案。

<!--more -->

## TPC-DS 数据模型
TPC-DS 使用零售业务建模，schema 包括客户、订单、销售、退货和产品数据等业务信息。标准测试模拟了任何成熟的决策支持系统都必须具备的两个重要组成部分:  

1. 用户查询: 数据分析、挖掘，可将运营事实转换为商业智能  
2. 数据维护: 可将管理分析过程与其所依赖的可操作外部数据源同步  

TPC-DS 使用星型、雪花模型建模，有多个维度和事实表。每个维度表都有主键，事实表使用外键与维度表主键关联。
维度表可以分为以下类型:  

1. 静态：表内容在数据库加载期间仅加载一次，且数据不随着时间变化而变化。  
2. 历史记录：为单个业务实体创建多行来保存维度数据更改的历史记录，每行包括创建/修改时间的列。"项目"是历史记录维度信息的一个范例。  
3. 非历史记录：不保留对维度数据所做更改的历史记录。随着维度数据的更新，之前保留的信息将被覆盖。所有事实表都与维度信息的最新值关联。"客户信息"是非历史记录维度信息的一个范例。  

TPC-DS 有 7 个事实表和 17 个维度表，平均每个表列数在 20 左右，且数据分布是真实而不均匀的，存在数据倾斜，与真实场景非常接近。因此TPC-DS成为客观衡量不同 Hadoop 版本以及 SQL on Hadoop 技术的最佳测试数据集。      
事实表包含 商店、仓库和互联网三个销售渠道中每个渠道的建模产品销售和退货事实表；库存事实表。   
维度表用于和每个销售渠道相关信息关联。  

注： ER 图设计及事实表和维度表表结构详见官方文档(specification.pdf)    
 
## TPC-DS 查询语句  
 
由标准建模的查询语句(q1~q99)具有以下特征：  

1. 解决复杂的业务问题 
2. 使用各种访问模式、查询短语、运算符和约束  
3. 使用会在各种查询过程之间变化的查询参数  

## 下载 TP-CDS kit
TPCDS  kit 是生成测试数据的工具  

方法一: 官网下载 http://www.tpc.org/tpc_documents_current_versions/current_specifications.asp     
	填写自己的邮箱后，官方会将下载链接发送到邮箱内，下载即可，文件包大小为 5MB 左右。   
方法二: github 下载 https://github.com/gregrahn/tpcds-kit/tree/master  
	
解压后目录数如下，  

```
tree -d v2.11.0rc2/  

v2.11.0rc2/
├── answer_sets
├── query_templates
├── query_variants
├── specification
├── tests
└── tools
```
其中 specification为 tpcds 说明书，包括工具说明、数据模型、ER 图、度量信息等；query\_templates 为查询语句模板；query\_variants 为查询语句模板的参数；answer\_sets 为查询结果集；tools 为数据集生成及查询语句生成工具集。Apache Spark sql/core module 内置了org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark 测试入口和 tpcds q1~q99 查询语句。因此只需将测试数据准备好即可。  

## 生成 TPC-DS

TPC-DS 可根据模拟场景产生以 GB 为最小粒度的数据集，常见的有 1TB、3TB、10TB、30TB、100TB等。  

### 编译 tpcds kit  
需要在每个节点上编译tpcds kit  

```
sudo yum install gcc make flex bison byacc git  
cd v2.11.0rc2/tools  
make clean && make OS=LINUX
```
编译完成后，在 tools 目录下会多出两个可执行文件: dsdgen,dsqgen。dsdgen 用于生成测试数据，dsqgen 用于生成在不同 SQL 标准下的查询语句。  

### 生成数据  

databricks 为 Spark SQL 制作的数据生成工具 spark-sql-perf，似乎不如其宣传的那么好用。  
spak-sql-perf github 地址:  https://github.com/databricks/spark-sql-perf    

其原理是: 依次为每个表 并行调用 tpcds kit的 dsdgen 命令生成数据(本地磁盘、text格式、'|'分割、无压缩) 作为表数据集，即生成的所有数据为 DataFrame、每个并行调用 dsdgen 产生的数据为 DataFrame 的分区。 然后将此数据集转换(读取再存储)为 hdfs上的 parquet+snappy 文件，并创建为 Hive 的外部表。核心代码如下    

调用 dsdgen 命令生成分区数据  
[DSDGEN.scala]

```
class DSDGEN(dsdgenDir: String) extends DataGenerator {
  val dsdgen = s"$dsdgenDir/dsdgen"
  def generate(sparkContext: SparkContext, name: String, partitions: Int, scaleFactor: String) = {
    val generatedData = {
      sparkContext.parallelize(1 to partitions, partitions).flatMap { i =>
        val localToolsDir = if (new java.io.File(dsdgen).exists) {
          dsdgenDir
        } else if (new java.io.File(s"/$dsdgen").exists) {
          s"/$dsdgenDir"
        } else {
          sys.error(s"Could not find dsdgen at $dsdgen or /$dsdgen. Run install")
        }
        val parallel = if (partitions > 1) s"-parallel $partitions -child $i" else ""
        val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir && ./dsdgen -table $name -filter Y -scale $scaleFactor -RNGSEED 100 $parallel")
        println(commands)
        BlockingLineStream(commands)
      }
    }
    generatedData.setName(s"$name, sf=$scaleFactor, strings")
    generatedData
  }
}
```

spark-sql-perf 拼接的并行 dsdgen 命令类似于:  cd tpcds/v2.11.0rc2/tools && ./dsdgen -table catalog\_sales -filter Y -scale 3000 -RNGSEED 100 -parallel 2000 -child 8，scala=3000意为生成 3000G数据集，-table= catalog_sales 当前生成表，-filter=Y 意为输出数据到 stdout，-RNGSEED=100 意为 RNG seed 值，-parallel=2000 意为并行度 2000 生成数据，-child 为当前并行的标识 用于命名本地数据文件。  

命令存在的问题(tpcds-kit 分支 master、v2.10、v2.5、v2.3均存在):  

1. tpcds kit 的识别的 option 为 _filter,但是 spark-sql-perf 拼接的命令为 -filter    
2. tpcds 数据模型之间有依赖关系，不能独立生成，表数据之间不是完全独立的，e.g. 销售表和退货表。 
为 table = catalog\_returns 生成数据时 dsdgen 会抛出错误 ERROR: Table catalog\_returns is a child; it is populated during the build of its parent (e.g., catalog\_sales builds catalog\_returns)   
  
尝试切换了 master、v2.10、 v2.5、 v2.3等分支 均无法解决上述问题。  
针对上述，我采用的方案是 依旧以 spark-sql-perf 为主体(表结构、dsdgen 生成数据、数据转换 parquet 到 hdfs、创建 hive 外部表等)，修改点如下： 

1. 修改数据并行生成命令   
         val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir && ./dsdgen -_filter Y -scale $scaleFactor -RNGSEED 100 $parallel")
2.  增加 并行数据生成后，把生成的数据上传到 hdfs 上的逻辑    
3.  修改表 DataFrame(RDD)生成逻辑，原有的逻辑为 dsdgen 命令包装为BlockingLineStream，修改为从 hdfs 上读取 $tableName*.dat 的逻辑
4.  text 文本转 parquet + snappy 逻辑不变、创建 hive 外部表关联到 parquet 文件的逻辑不变。  

3TB 测试数据集文件上传到 hdfs 后，使用 spark 来转换数据为 parquet 并注册外部表程序如下  
3TB 测试数据集使用 Spark 程序转换格式时，spark.executor.memory+spark.executor.memoryOverhead 不低于 10GB。  

```
spark-shell --master yarn-client --name "transformation" --queue high --executor-memory 8G --driver-memory 4G --num-executors 200 --conf "spark.executor.memoryOverhead=8G" --jars ~/tpcds/spark-sql-perf_2.11-0.5.1-SNAPSHOT.jar

import scala.sys.process._
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.immutable.Stream
import com.databricks.spark.sql.perf.BlockingLineStream
import com.databricks.spark.sql.perf.BlockingLineStream.Spawn 
import com.databricks.spark.sql.perf.BlockingLineStream.BlockingStreamed
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql._

val partitions = 200
val scaleFactor = "3000" 
val localToolsDir = "/home/jiulong.zhu/tpcds/v2.11.0rc2/tools"
val rootDir: String = "hdfs://pub1/user/jiulong.zhu/tpcds3t" 
val databaseName = "tpcds3t"
val sqlContext = new SQLContext(sc)
val format = "parquet"
sc.parallelize(1 to partitions,partitions).flatMap { i =>
        val parallel = if (partitions > 1) s"-parallel $partitions -child $i" else ""
        val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir && ./dsdgen -dir data/ -_filter Y -scale $scaleFactor -RNGSEED 100 $parallel -force Y")
        println(commands)
        val streamed = com.databricks.spark.sql.perf.BlockingLineStream.BlockingStreamed[String](true)
        val process = commands.run(BasicIO(false, streamed.process, None))
        Spawn(streamed.done(process.exitValue()))
        val hdfsCommand =  Seq(
          "bash", "-c",
          s"cd $localToolsDir && .hadoop fs -put data/*.dat temp/")
          val putProcess = hdfsCommand.run(BasicIO(false, streamed.process, None))
        Spawn(streamed.done(putProcess.exitValue()))          
        Seq(streamed.stream())
      }.collect()

// Run:
val tables = new TPCDSTables(sqlContext,
    dsdgenDir = localToolsDir, // location of dsdgen
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType

tables.genData(
    location = rootDir,
    format = format,
    overwrite = true, // overwrite the data that is already there
    partitionTables = true, // create the partitioned fact tables
    clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = 2000) // how many dsdgen partitions to run - number of input tasks.

// Create the specified database
sql(s"create database if not exists $databaseName")
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
```

tpcds 3TB 数据集在 text无压缩格式下为 2.7TB，转为 parquet snappy 格式后约为 900GB，orc snappy 格式约为 800GB。

## 测试 SparkSQL  

nohup sh TPCDSBench.sh &  
TPCDSBench.sh  

```
export SPARK_HOME=/home/jiulong.zhu/spark-2.3.0-bin-hadoop2.7
MASTER=yarn-client
APP_NAME=AE_ENABLED
DRIVER_MEMORY=8G
EXECUTOR_MEMORY=10G
NUM_EXECUTORS=100
EXECUTOR_CORES=5
EX_JARS=/home/jiulong.zhu/tpcds/spark-sql_2.11-2.3.1-SNAPSHOT-tests.jar
AE_ENABLE=false

spark-submit --master $MASTER --name ${APP_NAME}_${AE_ENABLE} \
--conf "spark.executor.instances=$NUM_EXECUTORS" \
--conf "spark.executor.cores=$EXECUTOR_CORES" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.sql.autoBroadcastJoinThreshold=21971520" \
--conf "spark.default.parallelism=1000" \
--conf "spark.sql.shuffle.partitions=1000" \
--conf "spark.sql.parquet.compression.codec=snappy" \
--conf "spark.sql.adaptive.enabled=${AE_ENABLE}" \
--conf "spark.dynamicAllocation.enabled=false" \
--conf "spark.executor.memoryOverhead=10G" \
--conf "spark.sql.adaptive.minNumPostShufflePartitions=1000" \
--queue high \
--driver-memory $DRIVER_MEMORY --executor-memory $EXECUTOR_MEMORY \
--class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark $EX_JARS \
--data-location hdfs://pub1/user/jiulong.zhu/tpcds3t
```

## 附录及参考

1. 从大数据平台二次开发、性能优化方面，准备一套合适的性能测试数据、通用查询语句、工具和测试流程对于性能提升量化和成果量化有非比寻常的意义。    
2. 可以试试 IBM 的 spark-tpc-ds-performance-test(https://github.com/IBM/spark-tpc-ds-performance-test)，但是仍旧需要自备一套 TPCDS 测试数据集。   
3. 参考:    
　　https://databricks.com/session/spark-sql-2-0-experiences-using-tpc-ds   
　　https://github.com/databricks/spark-sql-perf   


