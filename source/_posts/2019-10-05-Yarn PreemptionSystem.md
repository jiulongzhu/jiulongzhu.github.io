---
layout:     post  
title:      Yarn PreemptionSystem  
subtitle:   Yarn 抢占调度  
date:       2019-10-05  
author:     jiulongzhu  
header-img: img/moon_night.jpg  
catalog: true  
tags:  
    - Yarn 2.7.3   
    - 源码解析  
---


## 前言
Yarn 使用树状层级队列组织方式来管理资源,所有 NodeManager 持有的资源聚集作为资源根队列 root 来代表集群中所有可用资源,root可有有多个子队列,子队列也可以有自己的子队列,树状层级结构的最底层为叶子队列.除 root 外,每个子队列都会持有父队列一定比例的资源 即最低配额(Configured Capacity),以及最多可持有的资源 即最高配额(Configured Max Capacity).以上是 Yarn 的资源组织方式。     
Yarn 采用两阶段资源调度模型.第一阶段 客户端向 ResourceManager 申请资源启动 ApplicationMaster 作为 Application 的资源协调和 task 监控角色;第二阶段 计算框架驱动角色通过 AppliationMaster 向 ResourceManager 申请资源运行 Worker 角色。 ApplicationMaster 和 Worker角色资源申请及分配都是异步的过程,当 NodeManager 向 ResourceManager 汇报心跳时,附带了自身正在运行和已经完成的 Container 信息,并触发了资源调度器的 NodeUpdate 事件,调度器通过以下过程尝试分配资源     
1. 调度器委派 RootQueue 尝试进行资源分配  
2. RootQueue 委派其 ChildQueue(直至 LeafQueue) 尝试进行资源分配  
3. 遍历 Application 的 ResourceRequest,尝试进行分配或保留  
4. 若分配或保留成功,则同步 Queue/Application/NodeManager 信息.若分配成功,则触发 Container 的转变为 ALLOCATED 状态:RM 已经分配了资源给 APP(Attempt),但是 AM 此时还不知道,直至 AM 和 RM 的下次心跳时 AM 将对此 Container 做调度:下载依赖,准备运行环境,启动主类等;若保留成功,则该节点后续上报心跳时,将会跳过其分配调度,直至其可用资源量达到了保留资源的要求,则被保留的 Container 将会由 RESERVED 转换为 ALLOCATED 状态.保留资源固然在某种程度上算是浪费,但是保证了大 container 的 application 不会饿死      
5. NM 监控自身运行的所有 Container 状态,对于新启动和运行状态的 Container 以及运行完成的 Container 都会通过心跳告知 RM,RM 同步信息,更新各 Container 状态.若 Container 已经完成则在 RM 端同步 Queue/Application/NodeManager 信息  
以上是 Yarn 常规调度机制。         
层级队列中除 root 外,每个队列可以设置其拥有父队列的最低配额和最高配额.最高配额是队列无论在任何情况下都不会超出的用量,最低配额是在队列有任务时尽可能保证的配额.为了提高集群整体的资源利用率,Yarn 引入了抢占机制:  
1. 在队列中无任务运行时,队列没有必要硬遵守保留最低配额,可以将资源借给负载较高的其他队列     
2. 当借出资源的队列接收到了新提交的应用时,将出借的资源收回以满足资源需求      
以上是Yarn 抢占调度机制。第 1 点由 Yarn 资源调度器来实现;第 2 点只有调度器实现了 PreemptableResourceScheduler 接口,且抢占策略实现了SchedulingEditPolicy时才能实现   
本文侧重于第 2 点,并试图解决一些在小集群申请大容器的问题    

<!-- more -->

## 概述  

当队列 A 负载比较大时,可以临时借用负载较低队列 B 的资源.但如果有任务提交到 B 队列上,调度平台处理 B 借出的资源的方式一般有三种      

| 处理方式 | B 队列应用延时 | A 队列应用恢复代价 |浪费/重复工作量|
| ------------ | ------------ | ------------|------------|
| 等待应用完成 | 高 | 无 |无|
| 杀死应用             | 无 | 高 |高|
| 杀死应用部分容器    |无| 低| 一般 |

对于借出资源的处理,传统做法一般是等待 A 队列应用完成,将相应的资源还回,这样对于 B 队列应用程序来说有较高的延迟;直接杀死 A 队列应用是一种比较直接的做法,这对于一些特殊服务的影响很大 e.g. 7*24 的流式应用,优先级较高的服务(设计较好的公司会将这部分放在单独的队列甚至单独集群中以保证高可用).Yarn 抢占机制试图不彻底杀死整个应用,在考虑优先级和浪费工作量的情况下,从超分配队列中选出一些容器,释放掉资源达到还回原队列的目的   

## 源码解析

源码解析主要参照:  https://www.aboutyun.com/thread-24628-1-1.html ,与 Hadoop2.7.3版本有两处差异:  
1.Hadoop2.7.3 版本在 TempQueue 中加入了untouchableExtra变量,用以保存在队列不允许抢占时的(used absCapacity)值  
2.Hadoop2.7.3 版本在 计算各队列瓜分空闲资源时加了一些逻辑,在ProportionalCapacityPreemptionPolicy#computeFixpointAllocation中      

computeFixpointAllocation是抢占调度中最核心的逻辑: 计算每个队列的理想容量,决定了是否在该队列抢占及抢占数量的问题         
[ProportionalCapacityPreemptionPolicy.java]  

```
/**
* 重新计算每个子队列的理想配额,存储于每个子队列的 idealAssigned 成员变量中
* tot_guarant:父队列的理想配额
* qAlloc:父队列的子队列
* unassigned: tot_guarant 的 clone 对象,父队列的理想配额.将此配额分配给所有子队列
* ignoreGuarantee:该批次子队列是否配置了最低配额.将会影响到每个队列在分配空闲配额时的权重.
* 若为 true,没有配置最低配额,则每个队列均分空闲配额
* 若为 false,配置了最低配额,则每个队列按 最低配额/sum(最低配额) 的权重分配最低配额  
*/
private void computeFixpointAllocation(ResourceCalculator rc,
      Resource tot_guarant, Collection<TempQueue> qAlloc, Resource unassigned, 
      boolean ignoreGuarantee) {
      // 按照 欠分配的程度,做排序,下文引用了类内容
    TQComparator tqComparator = new TQComparator(rc, tot_guarant);
    PriorityQueue<TempQueue> orderedByNeed =
                                 new PriorityQueue<TempQueue>(10,tqComparator);
    for (Iterator<TempQueue> i = qAlloc.iterator(); i.hasNext();) {
      TempQueue q = i.next();
      if (Resources.greaterThan(rc, tot_guarant, q.current, q.guaranteed)) {
       // 在配置了队列可抢占后,untouchableExtra=0;若队列不可抢占,则超出最低配额的量算到理想配额内
        q.idealAssigned = Resources.add(q.guaranteed, q.untouchableExtra);
      } else {
        q.idealAssigned = Resources.clone(q.current);
      }
      Resources.subtractFrom(unassigned, q.idealAssigned);
      Resource curPlusPend = Resources.add(q.current, q.pending);
      // 欠分配的队列
      if (Resources.lessThan(rc, tot_guarant, q.idealAssigned, curPlusPend)) {
        orderedByNeed.add(q);
      }
    }
    // 欠分配的队列 瓜分 空闲配额  
    while (!orderedByNeed.isEmpty()
       && Resources.greaterThan(rc,tot_guarant, unassigned,Resources.none())) {
      Resource wQassigned = Resource.newInstance(0, 0);
      // 计算每个队列应得的权重  
      resetCapacity(rc, unassigned, orderedByNeed, ignoreGuarantee);
      // 使用tqComparator得到最欠分配的队列  依据是 idealAssigned/absCapacity
      //  超分配队列初始值为 1，欠分配队列初始小于 1 
      Collection<TempQueue> underserved =
          getMostUnderservedQueues(orderedByNeed, tqComparator);
      for (Iterator<TempQueue> i = underserved.iterator(); i.hasNext();) {
        TempQueue sub = i.next();
       // 当前轮次, 空闲配额*权重  
        Resource wQavail = Resources.multiplyAndNormalizeUp(rc,
            unassigned, sub.normalizedGuarantee, Resource.newInstance(1, 1));
        //  min{空闲配额*权重,最高配额-理想配额,需求配额}做理想配额的增量;下文引用了 offer 方法  
        Resource wQidle = sub.offer(wQavail, rc, tot_guarant);
        // 队列得到的最终值,即上面的 min{三元组}
        Resource wQdone = Resources.subtract(wQavail, wQidle);
	    // 在该轮得到了资源,那么可能还能得到资源,需要参加下一轮迭代;如果没得到资源,那么就不会再得到资源了(等于最高配额或满足了需求)
        if (Resources.greaterThan(rc, tot_guarant,
              wQdone, Resources.none())) {
          orderedByNeed.add(sub);
        }
        Resources.addTo(wQassigned, wQdone);
      }
      // 经过这一轮迭代后,全局空闲资源变化
      Resources.subtractFrom(unassigned, wQassigned);
    }
  }
  
  Resource offer(Resource avail, ResourceCalculator rc,
        Resource clusterResource) {
        // 最大配额-理想配额:表示该队列最多只能拿到这么多额外资源 
      Resource absMaxCapIdealAssignedDelta = Resources.componentwiseMax(
                      Resources.subtract(maxCapacity, idealAssigned),
                      Resource.newInstance(0, 0));
      Resource accepted = 
          Resources.min(rc, clusterResource, 
              absMaxCapIdealAssignedDelta,
              // avail:是传入值, 全局空闲资源*队列权重,表明在当前迭代 最多可以给的空闲资源
          Resources.min(rc, clusterResource, avail, 
           // current+pending-idealAssigned:表示 满足队列中所有应用程序需要的 额外资源 
          Resources.subtract(
              Resources.add(current, pending), idealAssigned)));
              
      Resource remain = Resources.subtract(avail, accepted);
      // 修改当前队列的理想配额  
      Resources.addTo(idealAssigned, accepted);
      return remain;
    }
```
欠分配比较器  
[TQComparator.java]  

```
static class TQComparator implements Comparator<TempQueue> {
    private ResourceCalculator rc;
    private Resource clusterRes;
    @Override
    public int compare(TempQueue tq1, TempQueue tq2) {
      if (getIdealPctOfGuaranteed(tq1) < getIdealPctOfGuaranteed(tq2)) {
        return -1;
      }
      if (getIdealPctOfGuaranteed(tq1) > getIdealPctOfGuaranteed(tq2)) {
        return 1;
      }
      return 0;
    }
    // 计算 idealAssigned / guaranteed
    private double getIdealPctOfGuaranteed(TempQueue q) {
      double pctOver = Integer.MAX_VALUE;
      if (q != null && Resources.greaterThan(
          rc, clusterRes, q.guaranteed, Resources.none())) {
        pctOver =
            Resources.divide(rc, clusterRes, q.idealAssigned, q.guaranteed);
      }
      return (pctOver);
    }
  }
```

## 抢占流程 

第一步: 递归获取所有队列的快照信息，重点包括 used/pending/absCapacity/maxAbsCapacity 信息  
第二步: 设置 root 队列的初始 idealAssigned 为 absCapacity(100%)，以此为依据将 root 队列的 idealAssigned 分摊给各层队列,即第三步     
第三步: 根据是否配置了最低配额将所有子队列分为两类，优先满足配置了最低配额的队列需求，然后剩余配额满足未配置最低配额队列的需求。对这两类队列进行资源再平衡 迭代计算出每个子队列的理想配额，即第四步。如果子队列的 used > absCapacity * (1+maxIgnoredOverCapacity)，则需要从其中掠夺资源，即第五步  
第四步: 首先按照当前使用状态(min{used,capacity})，计算出全局空闲资源量(弹性)，获得欠分配的队列；对欠分配的队列集合进行迭代以求出每个队列的 idealAssigned，在每一轮迭代中，设置每个欠分配队列所占的权重(capacity 为依据)，该权重影响队列在获取空闲资源时的增量；优先对欠分配程度最高的队列进行分配，配额为 min{全局空闲资源量 * 队列权重, maxAbsCapacity-idealAssigned, used+pending-idealAssigned},三元组中第一个值表示可分配给队列的额外配额，第二个值表示在不超过最高配额的条件下能接受的最多额外配额，第三个值表示满足应用程序资源需求的额外配额。若满足了队列的需求则将其剔除出欠分配的队列集合  
第五步: 如果队列 used > absCapacity * (1+maxIgnoredOverCapacity),那么将会释放掉 (used - idealAssigned) * naturalTerminationFactor (自然终止因子,默认0.2)的资源,则按照 <b>最小化影响 application </b>的原则(maxIgnoredOverCapacity 和 naturalTerminationFactor 都是遵从该原则)，优先释放最晚启动的 application 中的最晚分配的container，优先释放 reserved contaienr，优先释放非 AM 的 container。向常规调度器发送抢占 container 事件，并追踪这些 container 的存活时间，如果在超过了 maxWaitTime(15s)之后还没有释放，则强制常规调度器杀死   

## 一些误区

1. <b>最小化影响 application 原则是抢占过程中最优先考虑的原则</b>        
2. 抢占调度仅仅是所有队列的资源再平衡过程，<b>不为具体的应用程序调度，不为某个容器做分配或保留</b>。从设计模式原则的角度来看是合理的；抢占调度和常规调度松耦合，仅仅使用总线向常规调度器单向发送抢占/杀死容器事件，从架构分层分模块的角度来看也是合理的      
3. 抢占调度<b>不会平衡队列内部的用户资源使用量</b>，用户资源量的平衡应使用 user limit percent 或者 Fair 去做。e.g. user1 先向队列提交了 10 个 application,user2 后向队列提交了 2 个 application ,在抢占调度时 不会把 user1 占用的资源平衡给 user2 使用;且 如果队列要释放资源,那么由于 user2 的 application 后提交,反而会先于 user1 的 application 被抢占    
4. 抢占调度<b>不会使繁忙队列的资源达到最高配额，且不会使空闲队列在任意条件下都得到最低配额</b>。比如,队列 A 占用了队列 B 的所有资源，那么当 B 上有任务提交时，是 A 和 B 一起抢占 B 的最低配额资源，只不过 B 由于欠分配程度(idealAssigned/absCapacity)比较高，在抢占时多次迭代中都更优先于 A，体现出来就是 B 抢回了资源。在 B used + pending >> capacity 且 idealAssigned < capacity 时，B 的 idealAssigned 会尽快收敛到 capacity，然后和 A 一起瓜分剩余资源。        
5. 关于抢占的量，控制该值的配置主要是两个: yarn.resourcemanager.monitor.capacity.preemption.total\_preemption\_per\_round 每轮总抢占 默认 0.1，表示最多每轮抢占的资源占集群总资源的比例。若每轮抢占总量超过此值，则按比例在每个队列中缩减； yarn.resourcemanager.monitor.capacity.preemption.natural\_termination\_factor 自然终止因子 默认 0.2。考虑到容器自然终止的情况,即使不杀死容器也能在 5 * maxWaitTime(15s) 内收回 95%的资源，所以使用(used - idealAssigned)乘以该因子作为队列内最终抢占资源量，以尽量减少对 已启动 application 的影响  
6. 在 AM-RM 心跳协议 ApplicationMasterProtocol#allocate 中，AM 通过 AllocateRequest更新自己的需求，包括新增与释放 container。RM通过 AllocateResponse 响应请求，包括新分配的容器集合，释放的容器集合和<b>需要 AM 释放的被抢占容器集合</b>。抢占容器的消息类型分为两类:StrictPreemptionContract，AM 必须释放 RM 指定的容器；PreemptionContract。 AM 可以在满足同等大小资源的条件下灵活的替换 RM 指定的待抢占容器。目前 Yarn 采用的是 PreemptionContract类型，从 AM(用户)的角度来看这是可操作性很高的设计:待抢占的 worker 角色可能存储着 shuffle 文件、可能开启着重要的服务...... 恢复这些 worker 可能需要较多额外的工作，所以 RM 让 AM 去选择杀死哪些 container，只关注 AM 能不能还回等量的资源。但是抢占调度里面追踪着 RM 给 AM 指定的待抢占容器集合，如果 AM 替换掉这些容器，那么如果在抢占调度后续的周期发现这些追踪的容器存活大于 maxWaitTime，那么会直接杀死。这是一个有歧义的设计，目前 SparkOnYarn 的 YarnAllocator 没有对 RM 发给 AM 的待抢占容器集合做任何处理，即等同于等待容器超时被 RM 端杀死      
7. <b>抢占得到的资源可能不足以任何资源请求，但是抢占依旧会进行</b>。控制每轮抢占立即得到的资源总量的参数参考 "一些误区 5"。假设集群中只有两个叶子队列 A和B，最低配额都是 50%。初始状态，A 完全空闲，B 负载很高 抢占了 A 的所有资源。A 队列突然被提交了大量应用，那么在第一轮抢占时 B 是超分配的队列，idealAssigned 初值为50%，A是借出所有资源的队列 idealAssigned 初值为 0，但是 idealAssigned/absCapacity 小于 B，是最欠分配的队列 迭代时每轮都优先分配A，A 能得到的资源为: B 队列 (used 100% - idealAssigned 50%) *自然终止因子 0.2 =10%，且不能超过 集群资源100% * 每轮总抢占比例 0.1 = 10%。抢占资源量可能不足以满足任何 ResourceRequest，对于大型集群来说这个问题可能不足为虑，但是在小集群上可能会有一些难以预知的后果，尤其是在小集群上很大的资源需求(e.g. 40G)       

## 小集群大容器问题  

### 问题场景 

![](img/pictures/yarn_preempt/preempt_queue.png)   

为简单起见，Yarn 使用默认配置且在不考虑: 虚拟核，AM/Worker 的区别和分配/保留的区别。集群有三台服务器，每台服务器有 80G 内存，共计有 240G 的内存资源。有两个叶子队列: A 和 B，最低配额都是 50%，最高配额都是 100%。当 A 负载较高而 B 空闲时，A 使用了整个集群的资源，A 中只有一个应用 app\_A，启动的 container 都是 4G 大小。此时有新任务 app\_B 提交到队列 B，且需要的资源量大于 B 的最低配额，则在第一次抢占时 B 队列会获得(240-120) * 0.2 自然终止因子 = 24G 的资源，且该轮抢占最大资源量不能超过 240 * 0.1 每轮抢占资源量比例 = 24G，这些抢占的 contaienr 均匀分布在集群的所有节点上，即图中红色 container 为被抢占，绿色 container 为未被抢占    
 
- 场景 1: 假设 app\_B 待分配的 container 大小为 40G，每个节点上只有 8G 的空闲资源，不足以启动该 container。且即使这些被抢占的 container 都分布在一个节点上，也不足以启动 40G JVM。 抢占虽然发生，但是 B 没有使用，最终这些抢占释放的资源会被 app\_A 利用。这样就陷入了由周期性抢占导致的困局: 抢占调度 平衡资源-> B 无法利用资源-> A 可以利用资源->A 超分配,B 欠分配->抢占调度 平衡资源->.....  具体表现是: B 中的任务无法启动，A及app\_A 中最近启动的容器一直处于 被抢占->启动成为最新->被抢占......的循环中  

- 场景 2: 假设 app\_B 待分配的 container 大小为 20G，每个节点上只有 8G 的空闲资源，不足以启动 20G 的 container，也会陷入周期性抢占导致的困局。但是如果这些被抢占的 container 分布在一个节点上，足以启动 20G 的 JVM。这是因为 JVM 大小不高于每轮抢占资源总量且不大于每台服务器内存总量的缘故    

注: 常规调度器不希望一个容器占有节点太多的资源，所以对于大 container，不一定会做保留  
[LeafQueue.java]

```
boolean shouldAllocOrReserveNewContainer(FiCaSchedulerApp application,
      Priority priority, Resource required) {
    int requiredContainers = application.getTotalRequiredResources(priority);
    int reservedContainers = application.getNumReservedContainers(priority);
    int starvation = 0;
    if (reservedContainers > 0) {
      float nodeFactor = 
          Resources.ratio(
              resourceCalculator, required, getMaximumAllocation()
              );      
       // 使用所需节点的百分比来对大型容器施加偏差…
       // 防止需要使用整个节点的极端情况
      // Use percentage of node required to bias against large containers...
      // Protect against corner case where you need the whole node with
      // Math.min(nodeFactor, minimumAllocationFactor)
      starvation = 
          (int)((application.getReReservations(priority) / (float)reservedContainers) * 
                (1.0f - (Math.min(nodeFactor, getMinimumAllocationFactor())))
               );  
      if (LOG.isDebugEnabled()) {
        LOG.debug("needsContainers:" +
            " app.#re-reserve=" + application.getReReservations(priority) + 
            " reserved=" + reservedContainers + 
            " nodeFactor=" + nodeFactor + 
            " minAllocFactor=" + getMinimumAllocationFactor() +
            " starvation=" + starvation);
      }
    }
    return (((starvation + requiredContainers) - reservedContainers) > 0);
``` 

### 解决方案  

<b>方案 1</b>   
对于场景 1，可以从两个角度优化: 提高配置增加每轮抢占资源量及自然终止因子，以提高每轮抢占可以立即得到的资源量；规定应用程序单个容器的大小不能高于每轮抢占资源量。将场景 1 的问题转换问场景 2 的问题      
对于场景 2，尽可能使被抢占容器分布在同一个节点上，为"最小化影响 Application"，可以在每个节点上按照设定的选取规则尝试抢占容器(但是并未真的杀死)以满足大容器并计算抢占代价(sum(currentTime-containerStartTime))，选择抢占代价最低的节点抢占。  
 
 
优点:

1. 可以提高启动大容器的可能性，减少大容器调度延时    

缺点:  

1. 增加每轮抢占资源量和自然终止因子,将增加集群和队列资源在平衡点附近抖动程度(队列释放->阶梯式下降,队列获取->阶梯式上升)。最好配合yarn.resourcemanager.monitor.capacity.preemption.max\_ignored\_over\_capacity 属性使用。还是最小化影响原则，减小借用资源不多的队列的抖动      
2. 提高抢占配置，提供能够启动大容器的资源量，将增加了被杀死的容器数量，扩大了影响范围，恢复任务产生的无效工作量也会增加      
3. 尽可能在节点上抢占容器启动大容器，而不是在集群所有节点上抢占最晚启动的应用程序，导致这些节点资源利用率抖动极大  
4. 由于抢占调度和常规调度解耦，抢占调度在节点上抢占出大容器的空间，常规调度也未必在该空间内分配大容器  
  
仅限于抢占频率较低的场景，并使用某些参量(欠分配队列 totalPending, minResourceRequest)来决定 走默认抢占逻辑还是集中在节点上释放资源      

<b>方案 2</b>  

[资源预订系统](https://jiulongzhu.github.io/2019/10/28/Yarn-ReservationSystem/)
 
<b>方案 3</b>  
标签系统：对节点进行分类或分组的一种方式，应用程序可以指定在特定标签的节点执行。可以配置资源队列可以访问的节点标签集合，应用程序只能提交到这些包含该标签的队列上来使用这些具有标签的节点的计算资源  
yarn.scheduler.capacity.\<queue\_path\>.capacity 属性是<b>默认标签</b>(无标签)的资源配置，所有的队列都可以访问无标签的节点；yarn.scheduler.capacity.\<queue\_path\>.accessible-node-labels 属性用于指定队列可访问的标签节点，在不指定的情况下可以继承父队列的权限，若希望让某队列只能访问无标签节点，设置为空格即可；yarn.scheduler.capacity.\<queue\_path\>.accessible-node-labels.\<label\>.capacity 指定队列可以使用该标签资源的最低配额；yarn.scheduler.capacity.\<queue\_path\>.accessible-node-labels.\<label\>.maximum-capacity 指定队列可以使用该标签资源的最高配额     

适用场景 
 
1. 机构路由: BU A的任务，只能运行在具有 A 标签的 NM 上    
2. 服务路由: Hbase 的数据分析任务，只能运行在具有 Hbase 标签的 NM 上    
3. 特殊机器: 只有深度学习任务可以运行在具有 GPU 标签的 NM 上        

新增或修改一些节点，配置其标签，并设置仅有某些队列具有该标签资源的使用权限，大内存应用程序专用    

优点:
  
1. 可以运行大内存应用，且 SLA 高  

缺点:  
1. 标签资源空闲时其他应用无法使用，集群资源利用率相对较低  
2. 需要额外的服务器成本  
  
## 相关参数

- yarn.resourcemanager.scheduler.monitor.enable   
设置为 true ,启用抢占  
- yarn.resourcemanager.scheduler.monitor.policies  
启用抢占时,抢占策略主类.RM 将配置的主类启动在独立线程中,周期性的执行抢占.默认是org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy  
- yarn.resourcemanager.monitor.capacity.preemption.monitoring_interval  
周期性执行抢占策略的间隔,单位毫秒,默认是 3000  
- yarn.resourcemanager.monitor.capacity.preemption.max_wait_before_kill  
自容器被标记为被抢占到抢占策略强制杀死该容器的时间,单位毫秒,默认15000.留给 AM 灵活杀死 container 的时间,如果希望快速收回资源可以适当调小    
- yarn.resourcemanager.monitor.capacity.preemption.total_preemption_per_round  
每轮抢占最大资源总量占集群无标签资源的比例,默认 0.1. 即每轮抢占最多可以收回相当于集群无标签资源总量的10%,若每轮抢占资源量超过此值,则按比例在每个待抢占队列进行缩减    
- yarn.resourcemanager.monitor.capacity.preemption.max_ignored_over_capacity  
忽略抢占的阈值,默认 0.1.当队列资源使用量 used > absCapacity(1+0.1)时,认为其应该还回资源,才会抢占容器.用于避免资源消耗和配额的剧烈波动,最小化影响      
- yarn.resourcemanager.monitor.capacity.preemption.natural_termination_factor  
自然终止因子,统计发现值为 0.5 或者为 0 时,都会在 5* 15秒(上述第三配置项)内收回 95%的资源,默认 0.2.为每个队列设置抢占目标后,再乘以此值作为最终的抢占目标.假设第一周期,某队列待抢占值为 10G,那么最终抢占为 2G.第二周期待抢占值为 7G,最终抢占为 1.4G...以一种平滑的方式回收,降低波动.可以适当增加此值以加快回收速度  

## 一点思考 

1. 方案设计时考虑了很多，从尝试借鉴 CPU 抢占、内存 SWAP 模式、用户体验、架构分层等角度思考设计一个抢占调度系统的想法。有时间再整理吧     
2. 关于开源抢占系统通用优化的初步想法     
调度系统抢占的容器先不杀死，而是打一个标记，比如:MARK\_PREEMPTED\_KILL,在资源分配时优先使用这些标记的节点或者资源紧缺时使用 以尽可能使容器能自然终止(类似 jvm软引用)，无资源分配时则不杀死。其一可以保证 在周期性抢占造成的困局中，队列使用不到这些资源时，这些资源不会被反复无效调度，而是继续运行任务；其二 或许能提高资源抢占相应速度，代价是需要完善常规调度的逻辑，分配时如何处理MARK\_PREEMPTED\_KILL，保留时如何处理MARK\_PREEMPTED\_KILL...    

## 参考 
https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/NodeLabel.html  
https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html#Capacity\_Scheduler\_container\_preemption  
https://www.aboutyun.com/thread-24628-1-1.html



