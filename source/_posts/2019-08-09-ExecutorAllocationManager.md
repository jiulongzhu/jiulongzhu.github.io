---
layout:     post   				   
title:      ExecutorAllocationManager源码分析 	
subtitle:   Spark 源码分析 		
date:       2019-08-09 	
author:     jiulongzhu 						
header-img: img/moon_night.jpg  
catalog: true 				
tags:			
    - Spark 2.3.0
    - 源码解析
---

## 摘要

Spark 为 Standalone 和 CoarseGrained(Yarn/Mesos)调度模式提供了动态资源(executor)分配功能,可以根据 Application 的工作负载动态调整其占用的资源.在多 Application 同时运行的情况下,可以提高资源利用率.原理是Spark 定时调度,利用正在运行和积压的任务数推算出需要的executor 数量并和已持有的数量对比,增加 executor;利用维护的节点空闲时间信息,释放掉空闲的 executor.

<!-- more -->

## 背景

	在SparkOnYarn任务执行的时候,发现了下面几条日志:
	ExecutorAllocationManager: Requesting 2 new executors because tasks are backlogged (new desired total will be 7)
	ExecutorAllocationManager: Removing executor 6 because it has been idle for 180 seconds (new desired total will be 5)
	上述日志没有时间先后及因果关系,发现ExecutorAllocationManager 会依据积压的任务申请新的 executor,并在 executor 空闲一段时间后释放 executor.
	
## 开启方法

### Cluster

	仅以资源调度平台最通用的 yarn 举例
       1.spark 源代码编译时需支持 yarn 特性  
       2.将 $SPARK_HOME/common/network-yarn/target/scala-<version>/spark-<version>-yarn-shuffle.jar移动到各 NodeManager 的 classpath 下  
       3.各节点的yarn-site.xml 配置文件中,设置 yarn.nodemanager.aux-services为spark_shuffle,yarn.nodemanager.aux-services.spark_shuffle.class为org.apache.spark.network.yarn.YarnShuffleService
       4.etc/hadoop/yarn-env.sh文件中,增加NodeManager 的堆内存YARN_HEAPSIZE,以减少 shuffle 期间的 GC 频率
       5.重启所有的 NodeManager

### Application

	必须把下面两个参数默认都是 false,需要设置为 true
	spark.dynamicAllocation.enabled true		开启动态资源分配机制
	spark.shuffle.service.enabled true 	 使用额外的 shuffle service 服务,可以使 executor 被移除时,不会删除该 executor写入的 shuffle数据.
	spark.shuffle.service.port 7337   额外的 shuffle service 服务所占用的端口

### 配置参数	

spark.dynamicAllocation.enabled true	  
spark.shuffle.service.enabled true    
spark.dynamicAllocation.minExecutors 2		动态数量下界  
spark.dynamicAllocation.maxExecutors 100 	动态数量上界  
spark.dynamicAllocation.initialExecutors 2 	executor初始化时申请的数量   
spark.dynamicAllocation.schedulerBacklogTimeout 60s  被积压的任务等待时间超过此值时,触发一次executor申请   
spark.dynamicAllocation.sustainedSchedulerBacklogTimeout 60s 和spark.dynamicAllocation.schedulerBacklogTimeout值相同,用于后续的executor申请   
spark.dynamicAllocation.executorIdleTimeout 180s 	executor在空闲了该时间之后,释放该 executor   
spark.dynamicAllocation.cachedExecutorIdleTimeout infinity  缓存了数据的 executor 在空闲了该时间之后,释放该 executor.一般不修改即不删除   

## 源码解析
ExecutorAllocationManager 被调用的入口是 SparkContext

```
[SparkContext.scala]
    val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf) //判断 spark.dynamicAllocation.enable 参数
    _executorAllocationManager =
      if (dynamicAllocationEnabled) {
        schedulerBackend match {
          case b: ExecutorAllocationClient =>	//子类是 StandaloneSchdulerBackend 和 CoarseGrainedSchedulerBackend,没有 Local
            Some(new ExecutorAllocationManager(
              schedulerBackend.asInstanceOf[ExecutorAllocationClient], listenerBus, _conf,
              _env.blockManager.master))
          case _ =>
            None
        }
      } else {
        None
      }
    _executorAllocationManager.foreach(_.start())
```

ExecutorAllocationManager中的关键成员变量,主要用于申请/注销 executor 时的信息维护

```
[ExecutorAllocationManager.scala]
  //在下次申请 executor 时的滚动增量	
  private var numExecutorsToAdd = 1
  //当前已持有的 executor 数量.如果运行中所有的 executor 都挂了,该值正是 application 向 clustermanager 申请的 executor 数量,而非再从 initExecutorNum 和 numExecutorToAdd 滚动增加至挂掉前状态
  private var numExecutorsTarget = initialNumExecutors
  //已经请求释放但是还没被释放的 executor 列表
  private val executorsPendingToRemove = new mutable.HashSet[String]
  //application 所有的 executor
  private val executorIds = new mutable.HashSet[String]
  // A timestamp of when an addition should be triggered, or NOT_SET if it is not set
  // This is set when pending tasks are added but not scheduled yet
  private var addTime: Long = NOT_SET
   //<executorId,最大存活时间>信息.在 executor 第一次注册或不再运行任务时设置
  private val removeTimes = new mutable.HashMap[String, Long]
  //是否是 application初始化时期.为 true 时,不会申请executor;当开始执行任务后或者executor 已经过了超时时间 时为 false
  @volatile private var initializing: Boolean = true
  //用于 executor 本地化计算分配,调用 clustermanager 接口的必须参数
  private var localityAwareTasks = 0
  //用于 executor 本地化计算分配,调用cm 接口的必须参数
  private var hostToLocalTaskCount: Map[String, Int] = Map.empty
```

周期性检测 application 的工作负载,动态调节executor 的数量

```
ExecutorAllocationManager.scala
 def start(): Unit = {
    listenerBus.addToManagementQueue(listener)
    val scheduleTask = new Runnable() {
      override def run(): Unit = {
             ...
             schedule()
             ...
      	}
    }
    //定时调度,间隔100ms
    executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)
    //先向集群申请初始化数量的 executor. numExecutorTarget 初始值是 max(spark.dynamicAllocation.minExecutors,spark.dynamicAllocation.initialExecutors,spark.executor.instances),其他两个参数是 0 和 Map.Empty
    client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
  }
  
```
schedule 方法的逻辑分为两块,1.检测是否要申请 executor 以及申请 2.检测是否要释放 executor 以及释放

```
private def schedule(): Unit = synchronized {
    val now = clock.getTimeMillis
    //申请
    updateAndSyncNumExecutorsTarget(now)
    //待释放列表
    val executorIdsToBeRemoved = ArrayBuffer[String]()
    //保留未超时的 executor,将超时的 executor 加入到待释放列表
    removeTimes.retain { case (executorId, expireTime) =>
      val expired = now >= expireTime
      if (expired) {
        initializing = false
        executorIdsToBeRemoved += executorId
      }
      !expired
    }
    //释放
    if (executorIdsToBeRemoved.nonEmpty) {
      removeExecutors(executorIdsToBeRemoved)
    }
  }
```
先看释放 executor 的逻辑,比较简单

```
  private def removeExecutors(executors: Seq[String]): Seq[String] = synchronized {
    val executorIdsToBeRemoved = new ArrayBuffer[String]
    logInfo("Request to remove executorIds: " + executors.mkString(", "))
    //当前存活的节点数
    val numExistingExecutors = allocationManager.executorIds.size - executorsPendingToRemove.size
    var newExecutorTotal = numExistingExecutors
    executors.foreach { executorIdToBeRemoved =>
    //如果删除该节点,导致存活节点数小于动态资源下界或者小于当前需要的资源数,则不删除
      if (newExecutorTotal - 1 < minNumExecutors) {
        logDebug(s"Not removing idle executor $executorIdToBeRemoved because there are only " +
          s"$newExecutorTotal executor(s) left (minimum number of executor limit $minNumExecutors)")
      } else if (newExecutorTotal - 1 < numExecutorsTarget) {
        logDebug(s"Not removing idle executor $executorIdToBeRemoved because there are only " +
          s"$newExecutorTotal executor(s) left (number of executor target $numExecutorsTarget)")
      } else if (canBeKilled(executorIdToBeRemoved)) {
        executorIdsToBeRemoved += executorIdToBeRemoved
        newExecutorTotal -= 1
      }
    }
    if (executorIdsToBeRemoved.isEmpty) {
      return Seq.empty[String]
    }
    val executorsRemoved = if (testing) {
      executorIdsToBeRemoved
    } else {
        client.killExecutors(executorIdsToBeRemoved, adjustTargetNumExecutors = false,
        countFailures = false, force = false)
    }
     client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
    newExecutorTotal = numExistingExecutors
    if (testing || executorsRemoved.nonEmpty) {
      executorsRemoved.foreach { removedExecutorId =>
        newExecutorTotal -= 1
        logInfo(s"Removing executor $removedExecutorId because it has been idle for " +
          s"$executorIdleTimeoutS seconds (new desired total will be $newExecutorTotal)")
        executorsPendingToRemove.add(removedExecutorId)
      }
      executorsRemoved
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor/s " +
        s"${executorIdsToBeRemoved.mkString(",")} or no executor eligible to kill!")
      Seq.empty[String]
    }
  }
```
killExecutors 的逻辑在对接底层资源调度平台(Standalone,CoarseGrained)的实现类中,以CoarseGrainedSchedulerBackend为例

```
[CoarseGrainedSchedulerBackend.scala]
final override def killExecutors(
      executorIds: Seq[String],
      adjustTargetNumExecutors: Boolean,
      countFailures: Boolean,
      force: Boolean): Seq[String] = {
    val response = synchronized {
      val (knownExecutors, unknownExecutors) = executorIds.partition(executorDataMap.contains)
      unknownExecutors.foreach { id =>
        logWarning(s"Executor to kill $id does not exist!")
      }
     //从待删除的列表中过滤掉确定被删除但还未被删除的和没有正在跑task 的 executor,作为此次请求 cm 释放的executor
      val executorsToKill = knownExecutors
        .filter { id => !executorsPendingToRemove.contains(id) }
        .filter { id => force || !scheduler.isExecutorBusy(id) }
      executorsToKill.foreach { id => executorsPendingToRemove(id) = !countFailures }
      logInfo(s"Actual list of executor(s) to be killed is ${executorsToKill.mkString(", ")}")
      
      // If we do not wish to replace the executors we kill, sync the target number of executors
      // with the cluster manager to avoid allocating new ones. When computing the new target,
      // take into account executors that are pending to be added or removed.
      val adjustTotalExecutors =
        if (adjustTargetNumExecutors) {
          requestedTotalExecutors = math.max(requestedTotalExecutors - executorsToKill.size, 0)
          ..
          doRequestTotalExecutors(requestedTotalExecutors)
        } else {
          numPendingExecutors += knownExecutors.size
          Future.successful(true)
        }
        
      val killExecutors: Boolean => Future[Boolean] =
        if (!executorsToKill.isEmpty) {
          _ => doKillExecutors(executorsToKill)
        } else {
          _ => Future.successful(false)
        }
      val killResponse = adjustTotalExecutors.flatMap(killExecutors)(ThreadUtils.sameThread)
      killResponse.flatMap(killSuccessful =>
        Future.successful (if (killSuccessful) executorsToKill else Seq.empty[String])
      )(ThreadUtils.sameThread)
    }
    defaultAskTimeout.awaitResult(response)
  }
   protected def doKillExecutors(executorIds: Seq[String]): Future[Boolean] =
    Future.successful(false)
```
doKillExecutor和doRequestTotalExecutors在 CoarseGrainedSchedulerBackend 中有默认实现,里面内容比较有迷惑性,其实逻辑在YarnSchedulerBackend中,封装了YarnClientSchedulerBackend 和 YarnClusterSchedulerBackend 实现类的通用逻辑.ResourceManager 端的逻辑不在此讨论

```
[YarnSchedulerBackend.scala]
  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    yarnSchedulerEndpointRef.ask[Boolean](KillExecutors(executorIds))
  }
  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    yarnSchedulerEndpointRef.ask[Boolean](prepareRequestExecutors(requestedTotal))
  
```
再看申请 executor 的逻辑,对 ExecutorAllocationManager#start#schedule#updateAndSyncNumExecutorsTarget(now)方法做解析

```
private def updateAndSyncNumExecutorsTarget(now: Long): Int = synchronized {
    val maxNeeded = maxNumExecutorsNeeded
    if (initializing) {
      //application job 还没有开始执行第一个 stage,所以不需要申请额外的资源
       0
    } else if (maxNeeded < numExecutorsTarget) {
      //如果当前计算出所需的资源小于已经拥有的资源,那么没有必要再申请新的,申请了也是空闲
      //可以代入一些值做debug
      val oldNumExecutorsTarget = numExecutorsTarget
      numExecutorsTarget = math.max(maxNeeded, minNumExecutors)
      numExecutorsToAdd = 1

       //math.max(maxNeeded, minNumExecutors)与numExecutorsTarget且maxNeeded < numExecutorsTarget
      if (numExecutorsTarget < oldNumExecutorsTarget) {
        // We lower the target number of executors but don't actively kill any yet.  Killing is
        // controlled separately by an idle timeout.  It's still *helpful* to reduce the target number
        // in case an executor just happens to get lost (eg., bad hardware, or the cluster manager
        // preempts it) -- in that case, there is no point in trying to immediately  get a new
        // executor, since we wouldn't even use it yet.
        //同步当前需要的 executor 数量给 cm
        client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
        logDebug(s"Lowering target number of executors to $numExecutorsTarget (previously " +
          s"$oldNumExecutorsTarget) because not all requested executors are actually needed")
      }
      numExecutorsTarget - oldNumExecutorsTarget
    } else if (addTime != NOT_SET && now >= addTime) {
    //在 application 已经开始运行,申请过资源;且当前计算出来的需要的 executor 数量大于已有的 executor 数量;且任务已经被积压过(onSchedulerBacklogged事件会设置 add_time)那么会申请 executor
      val delta = addExecutors(maxNeeded)
      logDebug(s"Starting timer to add more executors (to " +
        s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
      addTime = now + (sustainedSchedulerBacklogTimeoutS * 1000)
      delta
    } else {
      0
    }
  }
  
  //依据当前stage正在运行的task数量和堆积的task数量来预估需要的executor数量
  //之所以是当前 stage,是因为onStageSubmitted和onStageCompleted方法会维护stageIdToNumTasks(hashmap),所以这个 map 里面存储的是DAG中无依赖关系的 stage-tasknum 数据
   private def maxNumExecutorsNeeded(): Int = {
    val numRunningOrPendingTasks = listener.totalPendingTasks + listener.totalRunningTasks
    (numRunningOrPendingTasks + tasksPerExecutor - 1) / tasksPerExecutor
  }
//申请 executor.如果当前需要的资源数超过动态最大资源数,该次不申请并且将 numExecutorsToAdd(申请 executor 的滚动增量)设置为1,而不是乘以2.
//numExecutorsToAdd:1->2->4->8->1(已持有的数量不小于动态最大数量,则没必要需要太快的增速)->2->4
private def addExecutors(maxNumExecutorsNeeded: Int): Int = {
    // Do not request more executors if it would put our target over the upper bound
    if (numExecutorsTarget >= maxNumExecutors) {
      logDebug(s"Not adding executors because our current target total " +
        s"is already $numExecutorsTarget (limit $maxNumExecutors)")
      numExecutorsToAdd = 1
      return 0
    }
	
    val oldNumExecutorsTarget = numExecutorsTarget
    // There's no point in wasting time ramping up to the number of executors we already have, so
    // make sure our target is at least as much as our current allocation:
    numExecutorsTarget = math.max(numExecutorsTarget, executorIds.size)
    // Boost our target with the number to add for this round:   滚动增量
    numExecutorsTarget += numExecutorsToAdd
    // Ensure that our target doesn't exceed what we need at the present moment:
    numExecutorsTarget = math.min(numExecutorsTarget, maxNumExecutorsNeeded)
    // Ensure that our target fits within configured bounds:
    numExecutorsTarget = math.max(math.min(numExecutorsTarget, maxNumExecutors), minNumExecutors)

    val delta = numExecutorsTarget - oldNumExecutorsTarget
    if (delta == 0) {
      // Check if there is any speculative jobs pending
      if (listener.pendingTasks == 0 && listener.pendingSpeculativeTasks > 0) {
        numExecutorsTarget =
          math.max(math.min(maxNumExecutorsNeeded + 1, maxNumExecutors), minNumExecutors)
      } else {
        numExecutorsToAdd = 1
        return 0
      }
    }
    val addRequestAcknowledged = try {
      testing ||
        client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
    } catch {
        ....
        false
    }
    if (addRequestAcknowledged) {
      val executorsString = "executor" + { if (delta > 1) "s" else "" }
      logInfo(s"Requesting $delta new $executorsString because tasks are backlogged" +
        s" (new desired total will be $numExecutorsTarget)")
      numExecutorsToAdd = if (delta == numExecutorsToAdd) {
        numExecutorsToAdd * 2
      } else {
        1
      }
      delta
    } else {
      logWarning(
        s"Unable to reach the cluster manager to request $numExecutorsTarget total executors!")
      numExecutorsTarget = oldNumExecutorsTarget
      0
    }
  }
```
资源申请

```
[YarnSchedulerBackend.scala]
override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    yarnSchedulerEndpointRef.ask[Boolean](prepareRequestExecutors(requestedTotal))
 }
```

## ExecutorAllocationListener
ExecutorAllocationListener监听器监听各类事件,触发相应的回调函数,对自身以及ExecutorAllocationManager 内维护的信息做修改,辅助 ExecutorAllocationManager 做动态资源调度.看一些回调函数的源码:

```
/**
   * Callback invoked when the specified executor has been added.
   */
private def onExecutorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      executorIds.add(executorId)
      // If an executor (call this executor X) is not removed because the lower bound
      // has been reached, it will no longer be marked as idle. When new executors join,
      // however, we are no longer at the lower bound, and so we must mark executor X
      // as idle again so as not to forget that it is a candidate for removal. (see SPARK-4951)
      executorIds.filter(listener.isExecutorIdle).foreach(onExecutorIdle)
      logInfo(s"New executor $executorId has registered (new total is ${executorIds.size})")
    } else {
      logWarning(s"Duplicate executor $executorId has registered")
    }
  }
  private def onExecutorRemoved(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      executorIds.remove(executorId)
      removeTimes.remove(executorId)
      logInfo(s"Existing executor $executorId has been removed (new total is ${executorIds.size})")
      if (executorsPendingToRemove.contains(executorId)) {
        executorsPendingToRemove.remove(executorId)
        logDebug(s"Executor $executorId is no longer pending to " +
          s"be removed (${executorsPendingToRemove.size} left)")
      }
    } else {
      logWarning(s"Unknown executor $executorId has been removed!")
    }
  }
  private def onSchedulerBacklogged(): Unit = synchronized {
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeoutS seconds)")
      addTime = clock.getTimeMillis + schedulerBacklogTimeoutS * 1000
    }
  }
  private def onExecutorIdle(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      if (!removeTimes.contains(executorId) && !executorsPendingToRemove.contains(executorId)) {
        val hasCachedBlocks = blockManagerMaster.hasCachedBlocks(executorId)
        val now = clock.getTimeMillis()
        val timeout = {
          if (hasCachedBlocks) {	//缓存了数据,shuffle write 数据等
            now + cachedExecutorIdleTimeoutS * 1000
          } else {
            now + executorIdleTimeoutS * 1000
          }
        }
        val realTimeout = if (timeout <= 0) Long.MaxValue else timeout // overflow
        removeTimes(executorId) = realTimeout
        logDebug(s"Starting idle timer for $executorId because there are no more tasks " +
          s"scheduled to run on the executor (to expire in ${(realTimeout - now)/1000} seconds)")
      }
    } else {
      logWarning(s"Attempted to mark unknown executor $executorId idle")
    }
  }
  private def onExecutorBusy(executorId: String): Unit = synchronized {
    logDebug(s"Clearing idle timer for $executorId because it is now running a task")
    removeTimes.remove(executorId)
  }
```

## 其他

>
动态资源调度设计的一点思考
1. 如何增加资源  
	负载与当前资源的权衡;资源增加算法:线性增加,对数等;资源快速满足需求快速下降  
2. 如何释放资源  
	如何判断空闲;资源上有不好处理的逻辑怎么办,数据或计算   
3. 设计模式   
	事件驱动的设计模式  
4. 解耦的重要性  
	曾经设计过一个主从式数据处理框架,但是在可拓展性上考虑的比较少.如果现在在那个系统上增加动态资源调度功能,在设计模式和解耦上比不上 spark 之万一...  
	
## 参考

 http://spark.apache.org/docs/2.3.1/configuration.html   
 http://spark.apache.org/docs/2.3.1/running-on-yarn.html#configuring-the-external-shuffle-service  









