---
layout:     post
title:      Improved MapPartitions In Spark
subtitle:   Spark mapPartitions 算子取巧用法  
date:       2019-11-22  
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - Spark 2.3.0
---


## 概述

map 和 mapPartitions 是 Spark 中最常用的 ETL 算子，用于将数据映射为另一批数据。区别在于,  
1.  数据粒度: map 输入和输出数据都是单条记录，严格的一对一关系。mapPartitions 输入和输出数据都是一个分区的数据迭代器, n 对 m 关系。  
2.  功能: mapPartitions 功能远强于 map,其一 mapPartitions 可以在映射过程中做诸如过滤的额外操作，其二在需要创建重量级对象(e.g. 数据库连接)的场景下 mapPartitions 比 map 操作更合适。 
3.  性能: 性能上无明显差异。一般将分区输出数据先存储在内存数据结构中,结束后转成迭代器形式。在内存中存储数据这种方式可能会导致 OOM，但是有方法避免，这是本文的价值点。     

<!-- more -->

foreach 和 foreachPartition 的关系/用法类似 map 和mapPartitions,区别在 foreach(Partition)是 action 算子而 map(Partitions)是 transformer 算子。  


## mapPartitions 一般用法

mapPartitions[V] 的输入参数是 f:(Iterator[U]=>Iterator[V]) 使RDD[U]映射为 RDD[V]  
一般用法是在每个分区内维护一个内存数据结构 暂存输出数据，使用输入数据迭代器遍历输入数据处理得到输出数据加入到内存数据结构中，处理完毕后将内存数据结构转换为输出数据迭代器。   

```
val rdd:RDD[Int] = ?
rdd.mapPartitions(it=>{
      val buffer = new ArrayBuffer[Int](64)
      while(it.hasNext){
        val next = it.next()
        if(???){
          buffer.+=(doSomething(next))
        }else{
          // abort
        }
      }
      buffer.iterator
    })
```
优点在于使用简便，缺点在于内存数据结构在数据量大时容易 OOM。  

## 场景 
使用 Spark 向数据库(mongodb)中写入数据时有量级不小的错误，需要将存储失败的数据另存。  

难点:   
1. 数据库的并发度支持不高,且其他业务也需要占用部分连接。所以最终 RDD 的分区数不能太大，进而导致单分区内数据很大。     
2.  单分区内数据很大，插入数据库的失败数据也很多，需要将失败数据转存到其他存储。大量失败数据维护在内存中容易导致 OOM。  

## improved mapPartitions 1
切入点在于 mapPartitions 参数是一个 迭代器向迭代器的映射函数   
以下代码为伪代码，仅作为模板使用   

```

/**
  * record by record 式的处理输入数据。
  * @param srcDataIterator 输入数据迭代器
  * @tparam T
  */
class TransformSaveDataIterator[T](srcDataIterator:Iterator[T]) extends Iterator[(T,Boolean)]{
 // 重量级对象最好在类内部维护，避免 driver->executor 序列化问题
  lazy val connection = createOrGetFromPoll()
  override def hasNext: Boolean = {
    if(srcDataIterator.hasNext){
      return true
    }else{
      closeResource()
      return false
    }
  }

  override def next(): (T, Boolean) = {
    val next:T = srcDataIterator.next()
    val result:Boolean = connectResource.doSave()  // try {doSomething;return true} catch {return false}
    (next, result)
  }

  private def closeResource(): Unit ={
    if(connectResource != null && connectResource.isActive()){
      connectResource.commit()
      connectResource.close()
    }
  }
}
```
优点在于解决了 OOM 问题，缺点在于需要 record by record 式处理数据，效率低下对于支持 batch 的数据库而言未能充分利用   

## improved mapPartitions 2
切入点借鉴 Spark Streaming 的 mini-batch，将输入数据的迭代器 slice 为多个，每个 slice 整批插入数据库中以利用 batch 功能并同时保存 slice 迭代器和事务执行结果。记录下所有输入数据的数量，当返回数据数量不大于该值时 可返回数据。每个 slice 内迭代器数据迭代完之后，切换为下一个迭代器直到返回数据量等于输入数据总量。  
以下代码为伪代码，仅作为模板使用 

```

/**
  * buffer 式处理输入数据
  * @param srcDataIterator 输入数据迭代器
  * @tparam T
  */
class TransformBufferedSaveIterator[T](srcDataIterator:Iterator[T]) extends Iterator[(T,Boolean)]{
  lazy val connection = createOrGetFromPoll()
  val BUFFER_SIZE = 2048
  lazy val buffer:ArrayBuffer[(Iterator[T],Boolean)] = new ArrayBuffer[(Iterator[T],Boolean)](128)
  var dataCnt:Int = 0  // total input data count
  var returnedCnt:Int = 0  // already returned data count
  val trigger = new AtomicBoolean(true)
  var bufferIterator:Iterator[(Iterator[T],Boolean)] = null
  var curIterator:(Iterator[T],Boolean) = null

  override def hasNext: Boolean = {
    if(trigger.get()){
      while(dataCnt < srcDataIterator.length){
        val slice:Iterator[T] = srcDataIterator.slice(dataCnt, dataCnt+BUFFER_SIZE)
        dataCnt +=slice.length
        val result:Boolean = doSave(slice)
        // new iterator or reverse iterator
        val traversableSlice = slice.slice(0, BUFFER_SIZE)
        buffer.+=:((traversableSlice,result))
      }
      closeResource()
      bufferIterator = buffer.iterator
      trigger.set(false)
    }
    returnedCnt < dataCnt
  }

  override def next(): (T, Boolean) = {
    if(curIterator!=null && curIterator._1.hasNext){
      dataCnt += 1
      return (curIterator._1.next(),curIterator._2)
    }else{
     // bufferIterator 最后一个 iterator 不会调用 next(此时hasNext()=false)
      curIterator = bufferIterator.next()
      next()
    }
  }

  def doSave(it:Iterator[T]): Boolean ={
    try{
      doSomething()
      connectResource.commit()
      buffer.clear()
      return true
    }catch{
      case e:Exception =>{
        return false
      }
    }
  }
  private def closeResource(): Unit ={
    if(connectResource != null && connectResource.isActive()){
      connectResource.commit()
      connectResource.close()
    }
  }
}
```

## SimpleTestCase
分别使用 improved mapPartitions 1 和 improved mapPartitions 2 做测试样例

```
object ImprovedMapPartition {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("").master("").config("","").getOrCreate();
    val sparkContext = spark.sparkContext
    val rdd:RDD[Int] = sparkContext.parallelize(1 to 20,3)
    // save record by record
    val resultRDD = rdd.mapPartitions(it=>{
      new TransformSaveDataIterator[Int](it)
    })
      resultRDD.cache()
    resultRDD.filter(_._2 == false) // exception data
      .saveAsTextFile("....")

    // save buffered records
    print("finally save records: "+ resultRDD.filter(_._2).count())
    val resultRDD2 = rdd.mapPartitions(it=>{
      new TransformBufferedSaveIterator[Int](it)
    })
      resultRDD2.cache()
    resultRDD2.filter(_._2 == false)
        .saveAsTextFile("....")
    print("finally save records: "+ resultRDD.filter(_._2).count())

    resultRDD.unpersist()
    resultRDD2.unpersist()
    spark.stop();
  }
}
```

