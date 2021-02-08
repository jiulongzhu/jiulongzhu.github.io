---
layout:     post
title:     Spark Executor 内存动态扩展方案
subtitle:  内存动态扩展
date:       2020-01-10
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - Spark 2.3.0
    - 方案
---

## 概述 

Spark Application 提交之后 Executor 内存被固定，业务环境上的长流程或长时处理任务经常由于 Executor 内存不足失败，导致人工干预、流程延迟且 Application 级恢复成本比较高。这种情况下，希望有套机制能够在 Executor 失败后再申请更大内存的 Executor 、对 Task 层面有更好的容错，在一定程度上减少 Task 由内存不足失败导致的 Application 失败恢复和人工干预成本。  

<!-- more -->

前置条件:   
1.  Spark On Yarn  
2. 开启动态资源分配(spark.dynamicAllocation.enabled)  

## 调度流程 

```mermaid
sequenceDiagram

RDD#action ->> SparkContext : 1.runJob(rdd,func,partitions,resultHandler)
SparkContext ->> DAGScheduler : 2. submitJob(rdd,func,partitions,resultHandler)
DAGScheduler ->> DAGScheduler : 3. JobSubmitted && createResultStage && getOrCreateParentStages && submitStages && submitMissingTasks
DAGScheduler ->> TaskScheduler : 4.submitTasks(createTaskSet)
TaskScheduler ->> SchedulerBackEnd : 5.reviveOffers

loop period & event
	SchedulerBackEnd ->> TaskScheduler : 6.makeOffers && resourceOffers
	Driver ->> Executor : 7.LaunchTask
end
SchedulerBackEnd -->> - RDD#action : 8. statusUpdate && callBack
```

## 主要思路  

1. Container(或 Executor) 由于内存超限被杀死的原因有两个: 物理内存(-104,137)/虚拟内存(-103,137)超限被 Yarn 资源监控线程杀死；堆内存 OOM 导致 JVM 进程被 kill，退出码是 143。退出码的不同决定了要增加的内存配置不同: -104,-103 和 137需要增加 Container 整体内存；143 需要增加堆内存。(YarnAllocator#allocateResource 拿到 Yarn 新分配的 container 和正常及异常退出的 container)   
2. 内存拓展的 Executor 启动参数修改: Executor 内存提升次数(默认0)，堆内存，全内存。主要用于 Executor 向 Driver 注册时标识自身的特征，该特征可以使 TaskScheduler将 OOM Task 调度到该 Executor。(YarnAllocator#allocator 拿到 Yarn 新分配的 container,ExecutorRunnable,CoarseGrainedExecutorBackend,RegisterExecutor 数据结构修改)    
3. 内存拓展的 Executor 注册/注销 ->> 额外维护的可运行 OOM Task 的 Executor 列表更新，ExecutorAdd、ExecutorLost、KillExecutor 等。(CoarseGrainedSchedulerBackEnd,ExecutorId和[提升次数,堆内存,全内存]的映射,提升次数和 ExecutorId 集合的映射)      
4. 普通 Task, OOM Task 失败的处理 ->> OOM Task失败则必须调度到内存拓展过的 Executor，已经在内存拓展过的 Executor 失败的 OOM Task 后要调度到更大或当前最大内存的 Executor，非 OOM 失败的 task 走现有容错调度(TaskSchedulerImpl)  
5. 大内存 ExecutorLost ->> 根据维护的ExecutorId 和[提升次数,堆内存,全内存]映射及 Container 退出码及内存拓展算法来决定下一次申请的 ContainerResource和 JVM 启动的 XMX    
6. 因动态资源分配功能超时退出 或 Yarn 抢占导致的 ExecutorLost在 YarnAllocator 使用大根堆或 最大值来记录资源信息,在下一次申请时不通过内存提升算法。   
7. OOM Task 容错: OOM Task 加入到一个特定的 pending 队列,以和普通错误 Task 做区分。TaskScheduler/ExecutorAdd/TaskCompletion 触发的 reviveOffer时,优先从特定 pending 队列调度 判断 ExecutorId 能否可能支撑该 Task(原所属失败的 Executor 提升内存次数必须小于当前 ExecutorId 提升内存次数)。若能则调度,若不能则不调度该任务,调度普通 pending 队列。  
8. 动态资源分配使用 running tasks和 pending tasks 及 spark.task.cores 来推测需要的总 Executor 数量。在系统的某时刻，可能有 OOM Task 但是却无内存拓展过的 Executor,推测可能也不需要新的 Executor。即: 当有 OOM Task 却无拓展内存的 Executor 时,需要在 CoarseGrainedSchedulerBackend 通过 ExecutorAllocationClient 的 requestTotalExecutor/requestExecutor 接口来触发申请 Executor。数量为 Max{当前 Executor数量/5,1},取 Max 是为了增加本地化可能性，多申请的资源由动态资源分配来管理。    
9. YarnAllocator 维护的状态需要有复原功能: OOM 时下一批次申请的 Executor 都是拓展过内存的，随后复原，以适配动态资源分配功能。YarnAllocator 取消堆积在 RM 的 ResourceRequest 时,优先取消没有拓展过内存的,不足则取消拓展过内存的 ResourceRequest。  

  
## Task 失败处理线  

1. DriverEndPoint 首先接收到任务 StatusUpdate 事件转交由 CoarseGrainedSchedulerBackEnd 处理  
2. CoarseGrainedSchedulerBackEnd 交由 TaskSchedulerImpl 处理 StatusUpdate 事件  
3. TaskSchedulerImpl 解析 StatusUpdate 事件的任务状态,并判断其成功/失败，若失败则需要交由 TaskSetManager 维护状态，加入到特定的失败堆积队列  
4. a.若此时全局无大内存 Executor，则需要绕过动态资源分配主动申请大内存 Executor，但此时全局无大内存 Executor 故暂不处理当前 OOM Task。 b.若有则阶梯式分配 OOM Task: 判断原有运行该 Task 的 Executor 提升次数，将失败任务分配到更高/最高提升次数的 Executor 上。若原有运行 Executor 内存已经最大，则依据加内存次数抛出异常终止 Application 或主动申请更大内存 Executor，暂不处理当前 OOM Task([3G,4G,5G] 3G上失败任务可指派给最高阶梯 5G,也可以指派给较高阶梯 4G)。  
5. 对某个 Executor 分配任务(TaskCompletion/ExecutorAdd)时，对 Executor 进行判断，在内存满足的情况下，优先分配特定失败堆积队列的任务，再指派 TaskSetManager 中普通任务或 pending 任务。 

## Executor 失败处理线    

基于 AM 和 RM 资源申请模式，AM updateResourceRequest 更新最新 ResourceRequest 给 RM，RM 响应 AM 的 allocateResource 请求通知新分配的 container,正常异常退出的 container。由此 AM 在新分配的 container 上启动新的 Executor，判断退出的 container 的退出码确定退出原因   

1. YarnAllocator 判断容器退出原因，若为 Yarn 抢占或 AM 主动杀死则记录到最大堆中，若为内存问题则使用内存拓展算法计算出下一次 updateResourceRequest 时 Container 配置、 Jvm 配置和内存拓展次数    
2. 动态资源分配或主动申请容器时 判断是否需要提升内存，若是则通过 updateResourceRequest 来修改;若否则使用大跟堆中的配置  
3. 动态资源分配当当前需要的 Executor 总量低于已有量时，取消 ResourceRequest 优先取消普通内存配置的 ResourceRequest，不足则取消内存拓展过的 Executor 

## 内存增加算法
  
1. 需要考虑 Yarn 规整(申请的 Container Resource 会被规整到 yarn.scheduler.minimum-allocation-mb 的整数倍)； 
2. 增加 Container 整体资源量时需要按比例分配(堆内:堆外默认为 10:1,可适当修改)；  
3. 从低内存跃迁至高内存的次数不能太大,即提升快然后迅速收敛以降低中间的失败尝试次数，可选择初始高增量快速降低的方式(e.g. 对数或固定系数，第一次提升0.5,第二次0.3,第三次 0.1,超出最大允许拓展次数后如果OOM 则快速抛出异常终止任务人为干预)。  

## 附录 


DAGScheduler 处理的事件类型:    
  
* JobSubmitted   
* JobCancelled  
*  JobGroupCancelled  
* AllJobsCancelled  
* MapStageSubmitted   
* ResubmitFailedStages  
* StageCancelled  
* ExecutorAdded  
* ExecutorLost  
* WorkerRemoved  
* TaskSetFailed  
* BeginEvent   
* CompletionEvent 
* SpeculativeTaskSubmitted  
* GettingResultEvent     
      
CoarseGrainedSchedulerBackEnd(Driver) 处理的事件类型:   

* RegisterExecutor  
* StopDriver  
* StopExecutors  
* RemoveWorker  
* RetrieveSparkAppConfig    
* StatusUpdate  
* ReviveOffers  
* KillTask  
* KillExecutorsOnHost  
* UpdateDelegationTokens  
* RemoveExecutor  
 
