---
layout:     post
title:      Yarn ReservationSystem
subtitle:   Yarn 预订系统, Yarn 预约系统, Yarn 保留系统  
date:       2019-10-28 
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - Yarn 2.7.3
    - 源码解析
---


## 概述

ReservationSystem 是 YARN ResourceManager 的组件。YARN 的 ReservationSystem 为用户提供了提前保留资源的能力,以确保重要的生产作业可预测地运行。ReservationSystem 执行仔细的准入控制，并保证绝对资源量(而不是群集大小的百分比)。保留具有组和并发的语义，并且可以有随时间变化的资源需求。  

<!-- more -->

## 工作流程  

![](/img/pictures/reservation/yarn_reservation_system.png)  

参考上图，预订资源的工作流程如下：  

* step 0: 用户提交预订创建请求，并收到包含 ReservationId 的响应。  
* step 1: 用户提交由 RDL(Reservation Definition Language)和 ReservationId 组成的 ReservationRequest。这描述了用户对资源(e.g. numContainer)和时间(e.g. duration)的需求。这可以通过常规的Client-to-RM协议(ApplicationClientProtocol)以编程方式完成，也可以通过RM的REST API来完成。如果提交的预订具有相同的ReservationId 并且RDL相同，则请求将成功但不会创建新的预订。如果RDL不同，则保留将被拒绝且请求失败
* step 2: ReservationSystem 委托 ReservationAgent(图中的GREE) 在计划(Plan)中为 ReservationRequest 找到一个合理的时间分配，计划(Plan)是一个跟踪当前所有已接受的预订请求以及系统中可用资源的内存数据结构。  
* step 3: SharingPolicy 提供了一种在预订请求上强制保证统计量的方法，决定接受或者拒绝预订。例如，CapacityOvertimePolicy允许强制保证用户可以在其所有预订中请求的瞬时最大容量，以及一段时期内对资源整体的限制，例如，用户所有的预订最多可以达到瞬时50％集群最大容量，但是在一天内，其平均值不能超过10％。(The SharingPolicy provides a way to enforce invariants on the reservation being accepted, potentially rejecting reservations. For example, the CapacityOvertimePolicy allows enforcement of both instantaneous max-capacity a user can request across all of his/her reservations and a limit on the integral of resources over a period of time, e.g., the user can reserve up to 50% of the cluster capacity instantanesouly, but in any 24h period of time he/she cannot exceed 10% average)  
* step 4: 成功验证后，ReservationSystem 会向用户返回一个ReservationId 作为票据    
* step 5: PlanFollower(线程周期调度)通过动态创建/调整/销毁队列将计划的状态发布到调度程序 
* step 6: 用户可以在(多个)应用程序的 ApplicationSubmissionContext 中指定 ReservationId 提交到可预订的队列(PlanQueue,具有 reservable 属性的 LeafQueue) 
* step 7: 常规调度器将从创建的特殊队列中提供容器,以确保遵守资源预定。在预订的时间和资源限制下，用户的(多个)应用程序可以以容量/公平的方式共享资源   
* step 8: 预订系统可以兼容容量下降的情况。包括拒绝之前接受最晚的预订兼容 reservable queue 的容量骤减，移动预订到 reservable queue 下的 default队列来兼容超时(预订到期但app 没结束)应用以重建计划  

注: step 8和官网解释不同。官网解释可能不实,参见源码解析-"step 8"

## 源码解析

### 涉及的类

* org.apache.hadoop.yarn.server.resourcemanager.reservation.AbstractReservationSystem  
该类继承了AbstractService,实现了ReservationSystem 并封装了Capacity/Fair 调度器下预订系统的核心实现  
主要功能是:作为服务启动时加载配置文件中配置的 reservable LeafQueue 转换为 Plan;管理 PlanFollower,确保 Plan与常规调度器的同步  

	| 方法  | 功能  | 备注 |
	|:------------- |:---------------:|:-------------|
	| setRMContext     | 保存 rmContext 指针 |    |
	| reinitialize     | 重新初始化 ReservationSystem |    |
	| getPlan     | 获取已被加载的 Plan |    |
	| getAllPlans    | 获取已被加载的所有 Plan  |    |
	| synchronizePlan     | 使用 PlanFollower 同步 Plan 与常规调度器 |    |
	| getPlanFollowerTimeStep     | PlanFollower 步长 |    |
	| getNewReservationId     | 获取一个全局唯一的reservationId |    |
	| getQueueForReservation     | 获取reservationId关联的队列 |    |
	| setQueueForReservation     | 为reservationId关联队列 |    |

* org.apache.hadoop.yarn.server.resourcemanager.reservation.AbstractSchedulerPlanFollower  
该类实现了 PlanFollower与 Runnable 接口  
主要功能是:周期性同步常规调度器与 Plan。通过将计划中的每个预订的当前资源映射到常规调度器(e.g. 队列的调整能力,设置池权重,调整应用优先级)，来影响调度器的资源分配 进而达到 保证作业与Plan 中预订一致的方式来使用资源。一个关键概念是将预订的绝对值式资源转换为队列的优先级和容量。PlanFollower 也会向 Plan 来同步集群总资源的变化使其作出相应的调整。  

	| 方法  | 功能  | 备注 |
	|:------------- |:---------------|:-------------|
	| init     | 通过 SystemClock,ResourceSchduler 和Plans 来初始化PlanFollower |    |
	| synchronizePlan     | 同步指定 Plan和常规调度器 | 周期性调用;时间紧迫时同步阻塞调用  |
	| setPlans     | 重置 PlanFollower 同步的 Plan 集合 |    |

* org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryPlan   
该接口实现了 Plan 接口,Plan 接口继承了PlanContext, PlanView, PlanEdit。实现只有 InMemoryPlan    
Plan 代表着预订系统的核心数据结构,维护着集群资源的工作安排(分配或收回)计划。用户将 ReservationRequest 提交给 RM 之后,RM 委托给 ReservationAgent，ReservationAgent 通过PlanView 接口咨询该 Plan 是否能满足 RDL 时间且资源约束。如果可以分配,则通过 PlanEdit 接口将其存储在该 Plan 中。之后便向用户返回 ReservationId 票据,用户可通过该票据在预订的时间范围使用预订的资源。  
PlanFollower将会周期性的从 Plan 中读取最新工作安排计划(队列瞬时容量),并同步给常规调度器,进而影响正在运行作业占用的资源。  
接口中有三类方法:  PlanContext 负责配置信息;PlanView 负责对Plan 状态的只读访问;PlanEdit 负责对 Plan 状态写入访问。  

	| 方法  | 功能  | 备注 |
|:------------- |:---------------:|:-------------|
| getStep     | 获取 Plan 的时间步长 | PlanContext,同 PlanFollower 的时间步长   |
| getReservationAgent  | 获取 Plan关联的 ReservationAgent   | PlanContext |
| getReplanner     | 使用 Planner 对象来应对Plan 的资源意外减少 | PlanContext   |
|getSharingPolicy | SharingPolicy 控制多用户共享计划资源| PlanContext | 
|getReservationById|通过 ReservationId 获取 Reservation 的详细信息|PlanView|
|getReservationsAtTime|获取指定时间点所有活跃的 Reservation 的详细信息|PlanView|
|getAllReservations|获取 Plan 中所有预订信息|PlanView|
|getTotalCommittedResources| 获取指定时间点所有预定的总资源量| PlanView|
|getConsumptionForUser|获取指定时间点指定用户预定的总资源量|PlanView|
|getEarliestStartTime|获取计划中最早的预订开始时间|PlanView|
|getLastEndTime|获取计划中最晚的预订结束时间|PlanView|
|addReservation|增加一个预订|PlanEdit|
|updateReservation|更新一个预订|PlanEdit|
|deleteReservation|删除一个预订|PlanEdit|
|archiveCompletedReservations|清除所有的过期预订|PlanEdit|

　　仅列举出核心方法,实际不限于此  

* org.apache.hadoop.yarn.server.resourcemanager.reservation.GreedyReservationAgent  
实现了ReservationAgent接口。 
一个简单的贪婪放置策略来满足用户预订的代理。具体方式是:按照 ReservationRequests中的各个 ReservationRequest作为单独的阶段,从deadline 开始向后移动至 arrival 来安排预订请求。该代理不考虑本地性,仅仅考虑容器粒度的验证(e.g. 不能超过最大容器大小)  

	| 方法  | 功能  | 备注 |
|:------------- |:---------------:|:-------------|
| createReservation     | 使用此代理尝试创建一个预订 | |
| updateReservation  | 使用此代理更新一个已有预订   | |
| deleteReservation     | 使用此代理删除一个已有预订 | |

* org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy  
主要功能是校验 Plan 能否接受用户预订请求 。实现类 CapacityOverTimePolicy使用容量的按时间拓展概念：策略会保证该用户的当前预订申请和已生效的预订申请资源不超过瞬时资源限制(e.g. reservable queue capacity * 1),且在 24h 时间窗口内不能超过平均资源限制(e.g. reservable queue capacity * 0.5)。从某种意义来说,预订可以使用 reservable queue 的大部分容量 但只要保证快速归还以保证平均资源限制，这可以防止资源滥用且增加了灵活性。通过配置瞬时资源限制和平均资源限制以及时间窗口,策略可以使 reservable queue 达到即时执行(max=100%,avg=100%)和完全灵活(max=?,avg=?,保留给其他用户或系统)的效果。   

	| 方法  | 功能  | 备注 |
|:------------- |:---------------:|:-------------|
| init     | 初始化 Policy | 策略必要的配置读取 |
| validate  | 校验 Plan 能否接受用户预订请求   | |
| getValidWindow     | 预订资源的过期时间 | 窗口为[-24h,当前)。预订的 deadline 在此之前的记录将被删除|

* org.apache.hadoop.yarn.server.resourcemanager.reservation.Planner  
实现类只有SimpleCapacityReplanner,且功能远不及 Planner 的设计(增删改 Plan)，只有删除预订资源的功能：从当前时刻开始直到 min{最后一个预约endtime，一个小时之后}，如果所有用户预订资源总量超过 reservable queue capacity，则删除接受时间较晚的一批预订。 

	| 方法  | 功能  | 备注 |
|:------------- |:---------------|:-------------|
| init     | 初始化 Planner |  |
| plan  | 更新现有的 Plan,或增删改已有预订或增加一个新的预订   | SCR 只支持删除晚的预订以保证预订资源总量不超过队列最低配额 |

* org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation  
预订系统的核心数据结构，是一种运行长度编码(Run Length Encoded)的稀疏数据结构(TreeMap)，可随着时间的推移维护资源分配。维护者各事件点(预订分配 starttime，endtime)时(Plan/当前预订)全局应有的资源总量。  

	| 方法  | 功能  | 备注 |
|:------------- |:---------------|:-------------|
| getEarliestStartTime     | 最早的资源分配的时间戳 | treemap  firstKey |
|getLatestEndTime|最晚的资源分配的时间戳|treemap lastKey |
|getCapacityAtTime|该时间点的 Plan 已预订资源量|treemap floorKey |
|addInterval|在treemap 中新增一个时间范围的预约资源记录|维护开始点和结束点的资源量|
|removeInterval|在 treemap 中删除一个时间范围的预约资源记录|维护开始点和结束点的资源量|
|addCompositeInterval|在 treemap 中新增一个时间范围的多个预订资源记录| never used |


>
 TreeMap 的floorEntry(targetKey) 和 lowerEntry(targetKey)  区别:   
 floorEntry 返回 key 小于等于 targetKey 的键值对,无则 null  
 lowerEntry 返回 key 严格小于 targetKey 的键值对,无则 null  

* org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation
实现自 ResourceAllocation。预订分配的结果，内存数据结构，包含预订的整体开始结束时间，ReservationSystem 校验(GA 校验资源/SharingPolicy 校验用户违规)通过 ReservationSubmissionRequest 的时间，<时间段，资源量>的分配细节和RLESparseResourceAllocation    

| 方法  | 功能  | 备注 |
|:------------- |:---------------|:-------------|
| compareTo     | 按 acceptTime比较,晚的在前| 比较器, Plan 队列资源骤减时,删除部分晚的的 ResourceAllocation |


* org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest  
客户端封装ReservationSubmissionRequest 请求，通过 client-RM 接口提交给 RM，数据结构如下       
 　　queueName:String    //支持预订资源的队列  
　　rd:ReservationDefinition //预订定义      
　　　　arrival:long  //预订开始时间的 最早时间    
　　　　deadline:long   //预订结束时间的 最晚时间  
　　　　name:String  //名称  
　　　　reservationRequests:ReservationRequests  //预订请求    　
　　　　　　reservationResources:List\<ReservationRequest\>  
　　　　　　　　capability:Capacity  //每个预订请求的资源量    
　　　　　　　　numContainers:int  //预订的 container 数量  
　　　　　　　　concurrency:int //并发度。numContainers 可以分批分配        
　　　　　　　　duration:long  //使用时间  
　　　　type:ReservationRequestInterpreter  //多个预订请求之间的依赖关系  

*  org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter  
枚举类型，用以表示多个预订请求之间的依赖关系(或称: 组关系)      

	| 类型  | 含义  | 场景 |
|:------------- |:---------------|:-------------|
|R_ANY| 仅满足 RDL 中一个预订请求即可|有多种等效的方式满足要求。e.g. 1个<4G,2core>或2个<2G,1core>。Agent 会决定使用最合适的ReservationRequest|
|R_ALL|需要满足 RDL中所有预订请求,请求的分配没有限制时间先后|事务|
|R_ORDER|需要满足 RDL 中所有预订请求,且有严格的时间限制。k 位置的分配时间段必须在 k+1位置分配时间段之前(无交集)，且 k 位置分配的结束时间和 k+1位置分配的开始时间可以有任意长的时间间隔|具有固定依赖的阶段性工作流。e.g. 第一个作业需要 1个<4G,2core> 5min，其输出作为第二个作业的输入，第二个作业需要 2 个<2G,1core> 10min，则两个作业预订的分配时间段必然不能重叠|
|R\_ORDER\_NO\_GAP|R_ORDER 的严格版本，要求 k 位置分配的结束时间和 k+1 位置分配的开始时间相同、不能有任何间隙,即 "zero-size gap","no\_gap"|1.当前一个作业输出规模比较大时,避免保留太长时间 2.实时性要求很高的场景,作业间时间差则增大了工作流累计延时|	

* org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse  
客户端提交 ReservationSubmissionRequest 请求后，若RM 校验通过可以分配预订，则返回 reservationId给客户端。数据结构如下  
　　reservationId:ReservationId  
　　　　clusterTimestamp:long    
　　　　id:long   
		
### step 0   
用户提交预订创建请求，并收到包含 ReservationId 的响应  

[ApplicationClientProtocol.java]

```
public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException
```
### step 1  
用户提交由 RDL(Reservation Definition Language)和 ReservationId 组成的 ReservationRequest。描述了用户对资源(e.g. numContainer)和时间(e.g. duration)的需求,可以通过常规的Client-to-RM协议(ApplicationClientProtocol)以编程方式完成，也可以通过RM的REST API来完成。如果提交的预订具有相同的ReservationId 并且RDL相同，则请求将成功但不会创建新的预订。如果RDL不同，则保留将被拒绝且请求失败  
以 TestCase 中的代码片段为例  

[TestClientRMService.java]

```
@Test
public void testReservationAPIs() {
	....
    //创建一个预订请求
    Clock clock = new UTCClock();
    //预订最早开始时间
    long arrival = clock.getTime();	   
    //使用时长 
    long duration = 60000;    
    //预订的最晚结束时间
    long deadline = (long) (arrival + 1.05 * duration);
    //RDL: Rervation Define Language 
    //请求 4 个 <1G,1core> container,使用时长是 6000ms,使用开始的最早时间是 arrival,使用结束的最晚内时间是 deadline。
    ReservationSubmissionRequest sRequest =
        createSimpleReservationRequest(4, arrival, deadline, duration);
    ReservationSubmissionResponse sResponse = null;
    try {
      sResponse = clientService.submitReservation(sRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    ....
}

private ReservationSubmissionRequest createSimpleReservationRequest(
      int numContainers, long arrival, long deadline, long duration) {
    ReservationRequest r =
        ReservationRequest.newInstance(Resource.newInstance(1024, 1),
            numContainers, 1, duration);
            //R_ALL 满足所有请求
    ReservationRequests reqs =
        ReservationRequests.newInstance(Collections.singletonList(r),
            ReservationRequestInterpreter.R_ALL);
    ReservationDefinition rDef =
        ReservationDefinition.newInstance(arrival, deadline, reqs,
            "testClientRMService#reservation");
    ReservationSubmissionRequest request =
        ReservationSubmissionRequest.newInstance(rDef,
            ReservationSystemTestUtil.reservationQ);
    return request;
  }
```
### step 2 
ReservationSystem 委托 ReservationAgent(图中的GREE) 在计划(Plan)中为 ReservationRequest 找到一个合理的时间分配，计划(Plan)是一个跟踪当前所有已接受的预订请求以及系统中可用资源的内存数据结构。   

[ClientRMService.java]

```
@Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {
    // 检查 ReservactionSystem 是否启用
    checkReservationSytem(AuditConstants.SUBMIT_RESERVATION_REQUEST);
    ReservationSubmissionResponse response =
        recordFactory.newRecordInstance(ReservationSubmissionResponse.class);
    // 通过 AtmicLong 创建全局唯一的ReservationId
    ReservationId reservationId = reservationSystem.getNewReservationId();
    // 第一步:  校验
    // 1.预订请求指定了 queue; 2.指定的 queue 是 reservable queue,即属于 ReservationSystem 管理  
    // 2.Reservation Define Language 校验
    //	2a. 空值检验  2b. deadline 不能早于当前时间 2c. ReservationRequests 空值及空集判断  
    //	2d. 分配所有请求的最短时间(R_ANY,R_ALL:取最大;R_ORDER..取和)不能超过 deadline-arrival 
    //	2e. 分配所有请求的最大资源量(concurrency * request capacity)不能超过 reservable queue capacity
    Plan plan =
        rValidator.validateReservationSubmissionRequest(reservationSystem,
            request, reservationId);
    // 校验 ACL	
    String queueName = request.getQueue();
    String user =
        checkReservationACLs(queueName,
            AuditConstants.SUBMIT_RESERVATION_REQUEST);
    try {
      // 第二步:  使用 ReservationAgent 来尝试放置预订请求
      boolean result =
          plan.getReservationAgent().createReservation(reservationId, user,
              plan, request.getReservationDefinition());
      if (result) {
        // 同步 reservationSystem <reservationId,queueName>关系
        reservationSystem.setQueueForReservation(reservationId, queueName);
        // create the reservation synchronously if required
        // 第三步:  如下,若预留的 arrival 时间早于当前(错过) PlanFollower 一个步长,则同步创建
        refreshScheduler(queueName, request.getReservationDefinition(),
            reservationId.toString());
  	// response 中返回放置预订请求的 reservationId
        response.setReservationId(reservationId);
      }
    } catch (PlanningException e) {
     ...
    }
    ...
    return response;
  }
   private void refreshScheduler(String planName,
      ReservationDefinition contract, String reservationId) {
    if ((contract.getArrival() - clock.getTime()) < reservationSystem
        .getPlanFollowerTimeStep()) {
      ....
      //后续再说源码
      reservationSystem.synchronizePlan(planName);
      ....
    }
```
在第二步中,RMClientService 委托 Plan(reservable queue 的映射) 绑定的 ReservationAgent 来决定是否放置该预订请求  
ReservationAgent(RA)将预订请求中的每个 ResourceRequest(RR)作为一个单独的 stage 尝试放置,并在放置时考虑 ReservationRequestInterpreter(RRI)组关系的处理。从最后一个 stage 开始向前依次放置，使用 Plan 已有的预订计划累计的数据和当次预订请求的累计中间数据判断能否放置当前 RR来影响当前 RR放置时间策略并存储最终放置策略结果。   
 
[GreedyReservationAgent.java]

```
private boolean computeAllocation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract,
      ReservationAllocation oldReservation) throws PlanningException,
      ContractValidationException {
    LOG.info("placing the following ReservationRequest: " + contract);
    // reservable queue capacity 
    Resource totalCapacity = plan.getTotalCapacity();
    
   // 译文:考虑在此增加逻辑来调整"ResourceDefinition" 来解决系统的缺陷(e.g. 大型容器的调度延迟) 
   //  TODO: 想不出会在这个位置使用什么样的策略来解决大容器调度延迟问题? 逻辑能转移到常规调度吗
    
    // 使用保守策略规整缩短 [arrival,deadline]区间
    long earliestStart = contract.getArrival();
    long step = plan.getStep();
    if (earliestStart % step != 0) {
      earliestStart = earliestStart + (step - (earliestStart % step));
    }
    long deadline =
        contract.getDeadline() - contract.getDeadline() % plan.getStep();
        
    // 将每个 ReservationRequest(RR) 作为一个单独的 stage,设置一些阶段间的临时变量
    long curDeadline = deadline;
    long oldDeadline = -1;
    // 当次预订请求，全部 RR 放置时间策略结果的(中间)数据结构 
    Map<ReservationInterval, ReservationRequest> allocations =
        new HashMap<ReservationInterval, ReservationRequest>();
     // 当次预订请求，全部 RR 放置造成<时间-资源>变动结果的(中间)数据结构  
    RLESparseResourceAllocation tempAssigned =
        new RLESparseResourceAllocation(plan.getResourceCalculator(),
            plan.getMinimumAllocation());

    List<ReservationRequest> stages = contract.getReservationRequests()
        .getReservationResources();
    ReservationRequestInterpreter type = contract.getReservationRequests()
        .getInterpreter();
    // 使用迭代器 从最后一个元素之后的 null 向前迭代
    for (ListIterator<ReservationRequest> li = 
        stages.listIterator(stages.size()); li.hasPrevious();) {
      ReservationRequest currentReservationStage = li.previous();
       //第一步:  校验 RR
      // 1.concurrency > 0; 2.numContainer>0; 3. numContainer 必须是 concurrency 的倍数; 4.单容器大小不得超过maximumAllocation
      validateInput(plan, currentReservationStage, totalCapacity);
      //第二步:  尝试分配单个 RR
      Map<ReservationInterval, ReservationRequest> curAlloc =
          placeSingleStage(plan, tempAssigned, currentReservationStage,
              earliestStart, curDeadline, oldReservation, totalCapacity);

      if (curAlloc == null) {
        // 组关系是 R_ALL,R_ORDER,R_NO_GAP 时，一个 RR 分配失败 导致整个事务失败
        // 组关系是 R_ANY,一个 RR 分配失败可以尝试其他 RR 
        if (type != ReservationRequestInterpreter.R_ANY) {
          throw new PlanningException("The GreedyAgent"
              + " couldn't find a valid allocation for your request");
        } else {
          continue;
        }
      } else {
       // 可以放置当前 RR(stage)
       
        allocations.putAll(curAlloc);
       // 组关系是 R_ANY 的话,可以终止后续 stage 的尝试了
        if (type == ReservationRequestInterpreter.R_ANY) {
          break;
        }

        // 如果组关系是 R_ODER,R_ODER_NO_GAP，则通过设置 curDeadline 来确保 RR 之间的先后关系 
        // 将下一个 RR 的 deadline 设置为当前 RR 分配中最早的开始时间
        if (type == ReservationRequestInterpreter.R_ORDER
            || type == ReservationRequestInterpreter.R_ORDER_NO_GAP) {
          curDeadline = findEarliestTime(curAlloc.keySet());

          // 对于 R_ORDER_NO_GAP,确认当前分配和前一个(后向)分配之间没有空隙
          // 空隙并非之前理解的完全相等,[t0,t1) 与 [t1,t2)之间的关系,而是不超过 Plan 的时间步长即可 
          if (type == ReservationRequestInterpreter.R_ORDER_NO_GAP
              && oldDeadline > 0) {
            if (oldDeadline - findLatestTime(curAlloc.keySet()) > plan
                .getStep()) {
              throw new PlanningException("The GreedyAgent"
                  + " couldn't find a valid allocation for your request");
            }
          }
          // keep the variable oldDeadline pointing to the last deadline we
          // found
          oldDeadline = curDeadline;
        }
      }
    }
    // 没有为预订请求分配到<时间,资源>,通过 throws exception 的方式来快速失败
    if (allocations.isEmpty()) {
      throw new PlanningException("The GreedyAgent"
          + " couldn't find a valid allocation for your request");
    }

  // 第三步:  创建预订
  // 在[arrival,earliestTime)之间加入了 "零填充",以表明其是从arrival time 开始的预订
  // TODO:在开头和末尾添加"零填充"的意义是什么?没有的话 会有什么问题  
    ReservationRequest ZERO_RES =
        ReservationRequest.newInstance(Resource.newInstance(0, 0), 0);
    long firstStartTime = findEarliestTime(allocations.keySet());
    if (firstStartTime > earliestStart) {
      allocations.put(new ReservationInterval(earliestStart,
          firstStartTime), ZERO_RES);
      firstStartTime = earliestStart;
      // 译: 考虑在[lastEndTime,deadline)添加"零填充"
    }
    // 译:这有待验证,其他代理可能也在放置(同步问题);有代理不知道的 SharingPolicy
    ReservationAllocation capReservation =
        new InMemoryReservationAllocation(reservationId, contract, user,
            plan.getQueueName(), firstStartTime,
            findLatestTime(allocations.keySet()), allocations,
            plan.getResourceCalculator(), plan.getMinimumAllocation());
    // 第四步: 新增或更新已有预订分配 。在 step 3 中解释
    if (oldReservation != null) {
      return plan.updateReservation(capReservation);
    } else {
      return plan.addReservation(capReservation);
    }
  }
```
再看上述 第二步:  尝试分配单个 RR    
这是整个流程中最重要的部分: 决定了用户的每个 RR 能不能放置,放置在哪个时间段,放置几个 container    
核心思想是: 从 deadline(动态 deadline)开始向后尝试，每次尝试都遍历 duration 时间，判断 duration 时间每个时间步长是否超过了 capacity(即判断 指定大小的资源能不能占用当前时间段，故称"放置")。然后向后移动一个时间步长，直至剩余时间不足以满足 duration 或者放置了 RR 中所有 container 结束       

[GreedyReservationAgent.java]  

```
private Map<ReservationInterval, ReservationRequest> placeSingleStage(
      Plan plan, RLESparseResourceAllocation tempAssigned,
      ReservationRequest rr, long earliestStart, long curDeadline,
      ReservationAllocation oldResAllocation, final Resource totalCapacity) {

    Map<ReservationInterval, ReservationRequest> allocationRequests =
        new HashMap<ReservationInterval, ReservationRequest>();

    Resource gang = Resources.multiply(rr.getCapability(), rr.getConcurrency());
    long dur = rr.getDuration();
    long step = plan.getStep();
    if (dur % step != 0) {
      dur += (step - (dur % step));
    }
    // 将一个并发作为一个原子性的组分配 
    // 每次分配资源量: gang=capacity * concurrency 
    //     共计分配次数: gangsToPlace= numContainers/concurrency  
    int gangsToPlace = rr.getNumContainers() / rr.getConcurrency();
    int maxGang = 0;

    // loop trying to place until we are done, or we are considering
    // an invalid range of times
    // 循环尝试分配 直至所有的 gang 原子组都分配完或者剩余时间不足以容纳一个 duration
    while (gangsToPlace > 0 && curDeadline - dur >= earliestStart) {

      // as we run along we remember how many gangs we can fit, and what
      // was the most constraining moment in time (we will restart just
      // after that to place the next batch)
      maxGang = gangsToPlace;
      long minPoint = curDeadline;
      int curMaxGang = maxGang;

      // 尝试在[curDeadline-duration,curDeadline) 长为 duration 的区间内放置未知个 Resource=gang 的原子组
      for (long t = curDeadline - plan.getStep(); t >= curDeadline - dur
          && maxGang > 0; t = t - plan.getStep()) {

        // 如果之前的预订存在的话,则删除此预订之前的分配(e.g. 更新预订时)
        Resource oldResCap = Resource.newInstance(0, 0);
        if (oldResAllocation != null) {
          oldResCap = oldResAllocation.getResourcesAtTime(t);
        }

	// 计算当前时间点的净可用资源
        Resource netAvailableRes = Resources.clone(totalCapacity);
        Resources.addTo(netAvailableRes, oldResCap);
        // Plan(reservable queue), 最靠近 t 时刻的累计容量
        Resources.subtractFrom(netAvailableRes,
            plan.getTotalCommittedResources(t)); 
        //当前预订, 最靠近 t 时刻的累计容量
        Resources.subtractFrom(netAvailableRes,
            tempAssigned.getCapacityAtTime(t));
        
        //计算当前时刻能满足的最大数量的 gang
        curMaxGang =
            (int) Math.floor(Resources.divide(plan.getResourceCalculator(),
                totalCapacity, netAvailableRes, gang));

        // min{需求的 gang 数量,剩余资源能支撑的 gang 数量}
        curMaxGang = Math.min(gangsToPlace, curMaxGang);
	// 记住当前尝试的 duration 内,最小数量的 gang 点(即 队列+reservation 占用资源量最多的点)，作为下次尝试的右边界
        if (curMaxGang <= maxGang) {
          maxGang = curMaxGang;
          //资源最紧俏的时间点。当资源充裕时,放置尽可能多的 gang,也会成为当前最紧俏的时间点，作为下次放置的右边界    
          minPoint = t;
        }
      }

      // if we were able to place any gang, record this, and decrement
      // gangsToPlace maxGang=0:没资源了 时间:范围不够了
      // 退出上一个循环过程有两种情况: 
      // 1.  maxGang=0,这个 duration 内有一个时间点 净剩余资源不足以放下一个 gang,那么整个 duration 都不能用了
      // 2. t<=curDeadline - dur 整个 duration 已经遍历完了。此时 maxGang > 0
      // 以上退出都有可能,所以需要中间变量 maxGang来做判断。
      if (maxGang > 0) {
      
      // 能在[curDeadline-duration,curDeadline) 内放置下 maxGang 个gang，即 concurrency * maxGang 个 container
        gangsToPlace -= maxGang;
        ReservationInterval reservationInt =
            new ReservationInterval(curDeadline - dur, curDeadline);
        ReservationRequest reservationRes =
            ReservationRequest.newInstance(rr.getCapability(),
                rr.getConcurrency() * maxGang, rr.getConcurrency(),
                rr.getDuration());
        // 记住已占用的空间。后续贴出源码
        // reservable queue 对应的 Plan 暂时是只读的，除非能将整个 ReservationRequests 放入到计划中。
        tempAssigned.addInterval(reservationInt, reservationRes);
        allocationRequests.put(reservationInt, reservationRes);
      }
      // 设置下次放置 gang 的右边界。右边界向右无法再安置一个 gang(如上逻辑所述),尝试在右边界向左放置新的 gang
      curDeadline = minPoint;
    }
    
    if (gangsToPlace == 0) {
     // 放置了所有的 gang 
      return allocationRequests;
    } else {
      // 不能为当前 ReservationRequest(RR) 在时间/容量/并发 限制上放置所有的 gang。
      // 此次 RR放置失败,如果不是 R_ANY 的话，可以终止整个 预订请求了。
      for (Map.Entry<ReservationInterval, ReservationRequest> tempAllocation :
        allocationRequests.entrySet()) {
        // 清理之前的中间数据。后续贴出源码
        tempAssigned.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());
      }
      return null;
    }
  
```

Plan 中使用 RLESparseResourceAllocation(以 TreeMap 为核心的稀疏数据结构)保存了当前已生效所有Reservation的累积<时间-资源>状态变化图，通过汇总所有 Reservation 的 RR 中每个分配的时间段(startTime,endTime)和资源量 最终得到了 Plan 随时间变化的已分配资源量。对每个 ReservationSubmissionRequest 请求来说 ，在尝试分配所有 RR 的时候，也使用RLESparseResourceAllocation 来保存临时分配好 RR 累积<时间-资源>状态变化图。  
 
[RLESparseResourceAllocation.java]  

```
private TreeMap<Long, Resource> cumulativeCapacity = new TreeMap<Long, Resource>();
//向 RLESparseResourcAllocation 中增加一个 <时间范围-容量> 的预订
public boolean addInterval(ReservationInterval reservationInterval,
      ReservationRequest capacity) {
    Resource totCap =
        Resources.multiply(capacity.getCapability(),
            (float) capacity.getNumContainers());
    if (totCap.equals(ZERO_RESOURCE)) {
      return true;
    }
    writeLock.lock();
    try {
      long startKey = reservationInterval.getStartTime();
      long endKey = reservationInterval.getEndTime();
      //截取 (?,endKey)之间 一段 TreeMap
      NavigableMap<Long, Resource> ticks =
          cumulativeCapacity.headMap(endKey, false);
      if (ticks != null && !ticks.isEmpty()) {
        Resource updatedCapacity = Resource.newInstance(0, 0);
        // 找到时间上小于等于 startKey 的最大 key 的键值对
        Entry<Long, Resource> lowEntry = ticks.floorEntry(startKey);
        if (lowEntry == null) {
	   // 表明 startKey 即为 RLE 全局最小键(最早)
          cumulativeCapacity.put(startKey, totCap);
        } else {
          updatedCapacity = Resources.add(lowEntry.getValue(), totCap);
          if ((startKey == lowEntry.getKey())
              && (isSameAsPrevious(lowEntry.getKey(), updatedCapacity))) {
             // 通过 remove 达到合并区间的。
             // e.g. [t0,t1)为 2G, [t1,t2)为 1G;现在在 [t1,t3) 上分配了 1G,那么可以删除 t1 这个点 
            cumulativeCapacity.remove(lowEntry.getKey());
          } else {
             //新增 一个时间点的容量 记录
            cumulativeCapacity.put(startKey, updatedCapacity);
          }
        }
        // [startKey,endKey)时间段内的所有已有记录都要增加 totCap 作为最终状态 
        // cumulativeCapacity 维护的是全局状态量,而不是增量或者单个 reservation 的数据
        Set<Entry<Long, Resource>> overlapSet =
            ticks.tailMap(startKey, false).entrySet(); //startKey 已经增加过了 
        for (Entry<Long, Resource> entry : overlapSet) {
          updatedCapacity = Resources.add(entry.getValue(), totCap);
          entry.setValue(updatedCapacity);
        }
      } else {
        cumulativeCapacity.put(startKey, totCap);
      }
      Resource nextTick = cumulativeCapacity.get(endKey);
      if (nextTick != null) {
        if (isSameAsPrevious(endKey, nextTick)) {
        // 合并 endKey
          cumulativeCapacity.remove(endKey);
        }
      } else {
	// endKey 时还回资源,所以在 endKey 前面记录都加了 totCap,此处会减去  
        cumulativeCapacity.put(endKey, Resources.subtract(cumulativeCapacity
            .floorEntry(endKey).getValue(), totCap));
      }
      return true;
    } finally {
      writeLock.unlock();
    }
  }
  // 在 RLESparseResourcAllocation 中删除一个 <时间范围-容量> 的预订
  public boolean removeInterval(ReservationInterval reservationInterval,
      ReservationRequest capacity) {
    Resource totCap =
        Resources.multiply(capacity.getCapability(),
            (float) capacity.getNumContainers());
    if (totCap.equals(ZERO_RESOURCE)) {
      return true;
    }
    writeLock.lock();
    try {
      long startKey = reservationInterval.getStartTime();
      long endKey = reservationInterval.getEndTime();
      NavigableMap<Long, Resource> ticks =
          cumulativeCapacity.headMap(endKey, false);
      //取出[startKey,endKey)区间内的所有记录
      SortedMap<Long, Resource> overlapSet = ticks.tailMap(startKey);
      if (overlapSet != null && !overlapSet.isEmpty()) {
        Resource updatedCapacity = Resource.newInstance(0, 0);
        long currentKey = -1;
        for (Iterator<Entry<Long, Resource>> overlapEntries =
            overlapSet.entrySet().iterator(); overlapEntries.hasNext();) {
          Entry<Long, Resource> entry = overlapEntries.next();
          currentKey = entry.getKey();
          // 在每个时间点的减去该预订已分配的资源量  
          updatedCapacity = Resources.subtract(entry.getValue(), totCap);
          cumulativeCapacity.put(currentKey, updatedCapacity);
        }
        // 左右边界点存在性判断
        Long firstKey = overlapSet.firstKey();
        if (isSameAsPrevious(firstKey, overlapSet.get(firstKey))) {
          cumulativeCapacity.remove(firstKey);
        }
        if ((currentKey != -1) && (isSameAsNext(currentKey, updatedCapacity))) {
          cumulativeCapacity.remove(cumulativeCapacity.higherKey(currentKey));
        }
      }
      return true;
    } finally {
      writeLock.unlock();
    }
  }
```

举个简单例子，为便于讨论:  
1.容量方面:  reservable queue 有 <2G,2core>的 capacity，所有预订请求中 container 容量都是<1G,1core>，即 queue 最多有 2 个container  
2.时间方面: 从 t0至 t5，时间单位是 PlanFollower 的时间步长 step(默认是 1s)。t0 为当前时刻    
3.请求方面: 已有的 reservation 不讨论 arrival,deadline，只给出 ReservationSystem 给出的分配<时间范围-容量>  
4.ReservationSystem 已有的预订如下  

| 预订 Id  | 开始时间  | 结束时间 | 占用时间 | 总资源量|
|:------------- |:---------------|:-------------|:-------------|:-------------|
|r0|t3|t4| 1 个单位| 1个 container|
|r1|t2|t3| 1个单位|2个 container|
|r2|t1|t2| 1个单位| 1个 container|

由此可以绘出 Plan 对应的 RLESparseResourceAllocation 中维护的累计<时间-资源>状态变化图  

![](/img/pictures/reservation/reservationSystem.png)  

即: r2 在 t1 时刻拿到1 个 container,因此图中 t1 时刻已分配资源状态是 1 个 container。  
      r2 在 t2 时刻释放 1 个 container,r1 拿到两个 2 个 container,因此 t2 时刻已分配资源状态是 2 个 container 
 
 假设 ReservationSystem接收到一个请求，ReservationDefinition定义的预订请求内容为：
arrival=t0,deadline=t5  
ReservationRequests(RRS) 中有一个 ReservationRequest(RR)   
　　capacity<1G,1core>,numContainer=2,concurrency=1,duration=2     
ReservationRequestInterpreter=R_ALL

来模拟一下GreedyRerservationAgent#placeSingleStage是如何尝试放置这一个 RR 的  
初始值:gang=concurrency * capacity=<1G,1core>  gangsToPlace=numContainers/concurrency=2  maxGang=2  
 
| 状态  | 循环开始前 maxGang  | 开始前 minPoint|开始前 curMaxGang| 循环变量 t|净可用资源(队列容量-Plan 累计-当前预订累计)|最终curMaxGang| 最终 maxGang|最终 minPoint| 是否进行下次循环|
|:------------- |:---------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|
|第一次循环|2| t5| 2 | t4| 2-0-0=2 contianer|2|2| t4|是|
|第二次次循环|2| t4| 2 | t3| 2-1-0=1 contianer|1|1| t3|否,duration 超限退出|

内层循环结束时，maxGang=1，即可以在[t3,t5)这个时间段放下 1 个 gang，在此为 1 个 container，外层循环将此分配封装成<ReservationInterval,ReservationRequest>加入到 当前预订累计分配的中间数据结构中(tmpAssigned:RLESparseResourceAllocation)。  

此时: gangsToPlace=1,curDeadline=t3,进行下一次外层循环  

| 状态  | 循环开始前 maxGang  | 开始前 minPoint|开始前 curMaxGang| 循环变量 t|净可用资源(队列容量-Plan 累计-当前预订累计)|最终curMaxGang| 最终 maxGang|最终 minPoint| 是否进行下次循环|
|:------------- |:---------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|
|第一次循环|1| t3| 1 | t2| 2-2-0=0 contianer|0|0| t2|否,maxGang=0退出|

内层循环结束后，maxGang=0，不能在[t1,t3)时间段放置任何一个 gang。  

此时: gangsToPlace=1,curDeadline=t2，进行下一次外层循环  

| 状态  | 循环开始前 maxGang  | 开始前 minPoint|开始前 curMaxGang| 循环变量 t|净可用资源(队列容量-Plan 累计-当前预订累计)|最终curMaxGang| 最终 maxGang|最终 minPoint| 是否进行下次循环|
|:------------- |:---------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|
|第一次循环|1| t2| 1 | t1| 2-1-0=1 contianer|1|1| t1|是|
|第二次循环|1| t1| 1 | t0| 2-0-0=2 contianer|1|1| t0|否,duration 超限退出|  

内层循环结束后，maxGang=1，即可以在[t0,t2)时间段放下一个 gang，即 1 个 container，外层循环将此分配封装好之后再次加入到 tmpAssigned 中。   

此时: gangsToPlace=0终止了外层循环。
得到的最终结论是: 在[t3,t5)时间段放下一个 container<1G,1core>,在[t0,t2)时间段放下一个 container<1G,1core>。该 RR 整体可以分配，满足组关系 R_ALL，即不考虑用户的情况可以分配该 RR。

### step 3 

SharingPolicy 提供了一种在预订请求上强制保证统计量的方法，决定接受或者拒绝预订。例如，CapacityOvertimePolicy允许强制保证用户可以在其所有预订中请求的瞬时最大容量，以及一段时期内对资源整体的限制，例如，用户所有的预订最多可以达到瞬时50％集群最大容量，但是在一天内，其平均值不能超过10％。(The SharingPolicy provides a way to enforce invariants on the reservation being accepted, potentially rejecting reservations. For example, the CapacityOvertimePolicy allows enforcement of both instantaneous max-capacity a user can request across all of his/her reservations and a limit on the integral of resources over a period of time, e.g., the user can reserve up to 50% of the cluster capacity instantanesouly, but in any 24h period of time he/she cannot exceed 10% average)  

在 step 2中，ReservationSystem 尝试对预订请求分配时间和资源。若分配成功，则尝试将其加入到 Plan 中或是更新 Plan。    

[GreedyReservationAgent.java] 

```
private boolean computeAllocation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract,
      ReservationAllocation oldReservation) throws PlanningException,
      ContractValidationException {
    LOG.info("placing the following ReservationRequest: " + contract);
    ......
    ReservationAllocation capReservation =
        new InMemoryReservationAllocation(reservationId, contract, user,
            plan.getQueueName(), firstStartTime,
            findLatestTime(allocations.keySet()), allocations,
            plan.getResourceCalculator(), plan.getMinimumAllocation());
    if (oldReservation != null) {
     // 更新
      return plan.updateReservation(capReservation);
    } else {
    // 新增
      return plan.addReservation(capReservation);
    }
    }
```
updateReservation 事务性更新 reservation:  先删除旧的reservation，再新增更新的 reservation(当次ReservationSubmissionRequest)。如果新增失败则再把旧的reservation 回滚。 
所以两处的逻辑的核心在于 addReservation 和 removeReservation。  

[InMemoryPlan.java]  

```
    //按时间段索引 ReservationAllocation 信息  
 private TreeMap<ReservationInterval, Set<InMemoryReservationAllocation>> currentReservations =
      new TreeMap<ReservationInterval, Set<InMemoryReservationAllocation>>();
   // Plan 整体的<时间-资源>状态变化图
  private RLESparseResourceAllocation rleSparseVector;
  // 细分用户的<时间-资源>状态变化图
  private Map<String, RLESparseResourceAllocation> userResourceAlloc =
      new HashMap<String, RLESparseResourceAllocation>();
  // 按 reservationId 索引ReservationAllocation 信息
  private Map<ReservationId, InMemoryReservationAllocation> reservationTable =
      new HashMap<ReservationId, InMemoryReservationAllocation>();
      
 public boolean addReservation(ReservationAllocation reservation)
      throws PlanningException {
    InMemoryReservationAllocation inMemReservation =
        (InMemoryReservationAllocation) reservation;
    if (inMemReservation.getUser() == null) {
      String errMsg =
          "The specified Reservation with ID "
              + inMemReservation.getReservationId()
              + " is not mapped to any user";
      LOG.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    writeLock.lock();
    try {
      if (reservationTable.containsKey(inMemReservation.getReservationId())) {
        String errMsg =
            "The specified Reservation with ID "
                + inMemReservation.getReservationId() + " already exists";
        LOG.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }
      // 第一步: 使用 SharingPolicy 校验 Plan 能否接受该 reservation。后续解释
      policy.validate(this, inMemReservation);
      // 第二步: 记录接受 reservation 的时间
      reservation.setAcceptanceTimestamp(clock.getTime());
      // 第三步: 内存数据结构维护
      ReservationInterval searchInterval =
          new ReservationInterval(inMemReservation.getStartTime(),
              inMemReservation.getEndTime());
      Set<InMemoryReservationAllocation> reservations =
          currentReservations.get(searchInterval);
      if (reservations == null) {
        reservations = new HashSet<InMemoryReservationAllocation>();
      }
      if (!reservations.add(inMemReservation)) {
        LOG.error("Unable to add reservation: {} to plan.",
            inMemReservation.getReservationId());
        return false;
      }
      currentReservations.put(searchInterval, reservations);
      reservationTable.put(inMemReservation.getReservationId(),
          inMemReservation);
      // 第四步: 维护Plan 整体的<时间-资源>状态变化图;维护细分用户<时间-资源>状态变化图  
      incrementAllocation(inMemReservation);
      LOG.info("Sucessfully added reservation: {} to plan.",
          inMemReservation.getReservationId());
      return true;
    } finally {
      writeLock.unlock();
    }
  
  private boolean removeReservation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());
    ReservationInterval searchInterval =
        new ReservationInterval(reservation.getStartTime(),
            reservation.getEndTime());
    Set<InMemoryReservationAllocation> reservations =
        currentReservations.get(searchInterval);
     // Plan 成员变量维护的信息 维护
    if (reservations != null) {
      if (!reservations.remove(reservation)) {
        LOG.error("Unable to remove reservation: {} from plan.",
            reservation.getReservationId());
        return false;
      }
      if (reservations.isEmpty()) {
        currentReservations.remove(searchInterval);
      }
    } else {
      String errMsg =
          "The specified Reservation with ID " + reservation.getReservationId()
              + " does not exist in the plan";
      LOG.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    reservationTable.remove(reservation.getReservationId());
    // 1.维护细分用户<时间-资源>状态变化图;维护Plan 整体<时间-资源>状态变化图  
    decrementAllocation(reservation);
    LOG.info("Sucessfully deleted reservation: {} in plan.",
        reservation.getReservationId());
    return true;
  }
```

Plan 在 addReservation 和 updateReservation 时都使用了 SharingPolicy 来校验用户资源使用是否违规。CapacityOverTimePolicy 是默认 SharingPolicy，主要校验两个方面资源使用是否违规：瞬时用量不超过 reservable queue capaciy 的 1%(默认);24h(默认)平均用量不超过reservable queue capacity 的 1%(默认)。上述的 1%和 24h 是源码内常量,无法用配置修改      

[CapacityOverTimePolicy.java]

```
public void validate(Plan plan, ReservationAllocation reservation)
      throws PlanningException {
    ReservationAllocation oldReservation =
        plan.getReservationById(reservation.getReservationId());
    if (oldReservation != null
        && !oldReservation.getUser().equals(reservation.getUser())) {
      throw new MismatchedUserException(
          "Updating an existing reservation with mismatched user:"
              + oldReservation.getUser() + " != " + reservation.getUser());
    }

    long startTime = reservation.getStartTime();
    long endTime = reservation.getEndTime();
    long step = plan.getStep();
    // reservable queue capacity 
    Resource planTotalCapacity = plan.getTotalCapacity();
    // 最大瞬时容量和最大平均容量	
    Resource maxAvgRes = Resources.multiply(planTotalCapacity, maxAvg);
    Resource maxInsRes = Resources.multiply(planTotalCapacity, maxInst);
    	
    IntegralResource runningTot = new IntegralResource(0L, 0L);
    // 平均(乘法,即积分面积)容量限制
    IntegralResource maxAllowed = new IntegralResource(maxAvgRes);
    maxAllowed.multiplyBy(validWindow / step);

    // 检查与该分配有重叠区域且长度为 validWindow(24h)的任何窗口,
    //提供给用户的资源是否超过瞬时容量和平均容量限制
    for (long t = startTime - validWindow; t < endTime + validWindow; t += step) {

      Resource currExistingAllocTot = plan.getTotalCommittedResources(t);
      Resource currExistingAllocForUser =
          plan.getConsumptionForUser(reservation.getUser(), t);
      Resource currNewAlloc = reservation.getResourcesAtTime(t);
      Resource currOldAlloc = Resources.none();
      if (oldReservation != null) {
        currOldAlloc = oldReservation.getResourcesAtTime(t);
      }
      // 所有用户累计 reservation 不能超过 reservable queue  capacity
      Resource inst =
          Resources.subtract(Resources.add(currExistingAllocTot, currNewAlloc),
              currOldAlloc);
      if (Resources.greaterThan(plan.getResourceCalculator(),
          planTotalCapacity, inst, planTotalCapacity)) {
        throw new ResourceOverCommitException(" Resources at time " + t
            + " would be overcommitted (" + inst + " over "
            + plan.getTotalCapacity() + ") by accepting reservation: "
            + reservation.getReservationId());
      }

      // 细分用户 容量不能超过 瞬时容量限制
      if (Resources.greaterThan(plan.getResourceCalculator(),
          planTotalCapacity, Resources.subtract(
              Resources.add(currExistingAllocForUser, currNewAlloc),
              currOldAlloc), maxInsRes)) {
        throw new PlanningQuotaException("Instantaneous quota capacity "
            + maxInst + " would be passed at time " + t
            + " by accepting reservation: " + reservation.getReservationId());
      }
      
      // 相当于增加了 (用户分配容量[已有的+当前reservation更新后的-当前reservation更新前的]) * (一个时间步长)的面积
      // 当前reservation更新前的 实际上在 已有的 之中,所以需要减去
      runningTot.add(currExistingAllocForUser);
      runningTot.add(currNewAlloc);
      runningTot.subtract(currOldAlloc);
	
      // 老化掉一个24h窗口之前的那个时间点的面积
      if (t > startTime) {
        Resource pastOldAlloc =
            plan.getConsumptionForUser(reservation.getUser(), t - validWindow);
        Resource pastNewAlloc = reservation.getResourcesAtTime(t - validWindow);
        runningTot.subtract(pastOldAlloc);
        runningTot.subtract(pastNewAlloc);
      }
      if (maxAllowed.compareTo(runningTot) < 0) {
        throw new PlanningQuotaException(
            "Integral (avg over time) quota capacity " + maxAvg
                + " over a window of " + validWindow / 1000 + " seconds, "
                + " would be passed at time " + t + "(" + new Date(t)
                + ") by accepting reservation: "
                + reservation.getReservationId());
      }
    }
  }
```
上述 CapacityOverTimePolicy 用积分面积的方式判断 是否超过平均容量限制的方式，很容易和 RLESparseResourceAllocation 混淆。  RLESparseResourceAllocation 存储的是<时间点-资源>的状态量,是累积状态量不是增量。而积分面积用的是 RLESparseResourceAllocation 绘成的状态图算的积分面积，每向后移动一个时间步长则增加 (RLE 那个时间点资源量) * (一个时间步长) 的积分面积，并减去(RLE 24h前那个时间点资源量) * (一个时间步长)的积分面积。从而达到随着时间增加，积分面积始终是 RLESparseResourceAllocation 图中 [now-24h,now]这个区间的面积，然后和(平均资源限制容量) * (24h) 来比较。很巧妙，第一次见定积分可以在程序中这么用           

### step 4

成功验证后，ReservationSystem 会向用户返回一个ReservationId 作为票据
参考 "step 1" 的接口定义和 "step 2" ReservationAgent 分配成功后的处理  
 
### step 5

PlanFollower(线程周期调度)通过动态创建/调整/销毁队列将计划的状态发布到调度程序   

PlanFollower(Runnable) 随 ResourceManager 初始化时创建的 ReservationSystem(服务) 初始化而初始化 启动而启动，调用栈如下  

->ResourceManager$RMActiveServices#serviceInit  
　　->ResourceManager#createReservationSystem  
　　　　->AbstractReservationSystem#serviceInit  
　　　　　　->AbstractReservationSystem#createPlanFollower  

[AbstractReservationSystem.java]　

```
  public void serviceInit(Configuration conf) throws Exception {
    Configuration configuration = new Configuration(conf);
    // 第一步: 初始化所有的 reservable queue 。
    // 指定 SharingPolicy,Planner,capacity,planstep...
    reinitialize(configuration, rmContext);
    // 第二步: 创建 PlanFollower。加载类:CS 调度器对应 CapacitySchedulerPlanFollower...
    planFollower = createPlanFollower();
    if (planFollower != null) {
      // 第三步:初始化 PlanFollower 
      planFollower.init(clock, scheduler, plans.values());
    }
    super.serviceInit(conf);
  }
  
  public void serviceStart() throws Exception {
    if (planFollower != null) {
      scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
      //定时调度,调度周期是 planStepSize(默认1s)
      scheduledExecutorService.scheduleWithFixedDelay(planFollower, 0L,
          planStepSize, TimeUnit.MILLISECONDS);
    }
    super.serviceStart();
  }
```
PlanFollower的核心逻辑在 AbstractSchedulerPlanFollower，用于 PlanFollower 与常规调度器同步预订分配的信息，下有 CapacitySchedulerPlanFollower 和 FairCapacityPlanFollower 两个子类。  

[AbstractSchedulerPlanFollower.java] 

```
@Override
  public synchronized void run() {
    for (Plan plan : plans) {
      synchronizePlan(plan);
    }
  }

@Override
  public synchronized void synchronizePlan(Plan plan) {
     String planQueueName = plan.getQueueName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running plan follower edit policy for plan: " + planQueueName);
    }
    long step = plan.getStep();
    long now = clock.getTime();
    if (now % step != 0) {
      now += step - (now % step);
    }
    Queue planQueue = getPlanQueue(planQueueName);
    if (planQueue == null) return;
    
    Resource clusterResources = scheduler.getClusterResource();
    // plan 对应的 reservable queue 的容量
    Resource planResources = getPlanResources(plan, planQueue,
        clusterResources);
    // 当前正在生效的预订(当前时间介于 startTime 和 endTime 之间的所有 Reservation) 
    Set<ReservationAllocation> currentReservations =
        plan.getReservationsAtTime(now);
    // 当前正在生效的 reservationId 集合
    Set<String> curReservationNames = new HashSet<String>();
    // 当前正在生效预订的 resource
    Resource reservedResources = Resource.newInstance(0, 0);
    // 当前正在生效的 reservation;
    int numRes = getReservedResources(now, currentReservations,
        curReservationNames, reservedResources);

    // 创建 reservable queue 的默认队列: xxx-default
    String defReservationId = getReservationIdFromQueueName(planQueueName) +
        ReservationConstants.DEFAULT_QUEUE_SUFFIX;
    String defReservationQueue = getReservationQueueName(planQueueName,
        defReservationId);
    createDefaultReservationQueue(planQueueName, planQueue,
        defReservationId);
    curReservationNames.add(defReservationId);

    // 第一步: 若正在生效的预约资源量大于队列容量，则删除晚接受的部分预约(accept_time)
    // 如果专用于预订资源的队列骤减(或配置或节点宕机),通过调用 Planner 来删除部分已有预约
    if (arePlanResourcesLessThanReservations(clusterResources, planResources,
        reservedResources)) {
      try {
        plan.getReplanner().plan(plan, null);
      } catch (PlanningException e) {
        LOG.warn("Exception while trying to replan: {}", planQueueName, e);
      }
    }
    
    // 第二步: 标记当前预约中，哪些旧的预约需要过期删除，哪些新的预约需要创建队列  
    List<? extends Queue> resQueues = getChildReservationQueues(planQueue);
    Set<String> expired = new HashSet<String>();
    for (Queue resQueue : resQueues) {
      String resQueueName = resQueue.getQueueName();
      String reservationId = getReservationIdFromQueueName(resQueueName);
      if (curReservationNames.contains(reservationId)) {
        // 当前活跃的预订包含此此预约，因此不用为它创建对应的队列
        curReservationNames.remove(reservationId);
      } else {
	// 预约已经结束，准备清除
        expired.add(reservationId);
      }
    }
    // 第三步: 清除过期预约。 getMoveOnExpiry 默认为 true
    cleanupExpiredQueues(planQueueName, plan.getMoveOnExpiry(), expired,
        defReservationQueue);

    float totalAssignedCapacity = 0f;
    if (currentReservations != null) {
      // 释放默认队列中的所有多余容量  
      try {
        setQueueEntitlement(planQueueName, defReservationQueue, 0f, 1.0f);
      } catch (YarnException e) {
        LOG.warn(
            "Exception while trying to release default queue capacity for plan: {}",
            planQueueName, e);
      }
       // 第四步: 增加新的预约或更新已有的预约 对应的队列配额
       // 4.1 首先按照(当前时间待分配的容量 - 已有的容量)从小到大(从负到正)的方式排序一次调整 capacity。
       // 这种排序方式用以避免分配过程中瞬时容量超出 100%的 capacity(猜测可能是为了减少干扰并发预订和抢占介入)
      List<ReservationAllocation> sortedAllocations =
          sortByDelta(
              new ArrayList<ReservationAllocation>(currentReservations), now,
              plan);
      // 4.2 为每个队列设置或更新 capacity
      for (ReservationAllocation res : sortedAllocations) {
        String currResId = res.getReservationId().toString();
        if (curReservationNames.contains(currResId)) {
          // 每个reservationId 对应着常规调度器中 PlanQueue 下的一个 ReservationQueue
          addReservationQueue(planQueueName, planQueue, currResId);
        }
        // 当前生效的 ResourceAllocation 需要(或RS已分配给Reservation)的 capacity
        Resource capToAssign = res.getResourcesAtTime(now);
        float targetCapacity = 0f;
        if (planResources.getMemory() > 0
            && planResources.getVirtualCores() > 0) {
          // 绝对值式的容量相对 PlanQueue capacity 计算相对容量，capToAssign/planResources
          targetCapacity =
              calculateReservationToPlanRatio(clusterResources,
                  planResources,
                  capToAssign);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Assigning capacity of {} to queue {} with target capacity {}",
              capToAssign, currResId, targetCapacity);
        }
        // set maxCapacity to 100% unless the job requires gang, in which
        // case we stick to capacity (as running early/before is likely a
        // waste of resources)
        // 设置最大配额。感觉是无效代码，不明白为什么要设置最大配额 都为1不行吗，为什么按有无并发来设置最大配额  
        float maxCapacity = 1.0f;
        if (res.containsGangs()) {
          maxCapacity = targetCapacity;
        }
        try {
         // 设置capacity 和 maxCapacity
          setQueueEntitlement(planQueueName, currResId, targetCapacity, maxCapacity);
        } catch (YarnException e) {
          LOG.warn("Exception while trying to size reservation for plan: {}",
              currResId, planQueueName, e);
        }
        totalAssignedCapacity += targetCapacity;
      }
    }
    // 第五步: 设置 default queue 的配额(PlanQueue 的剩余配额)
    float defQCap = 1.0f - totalAssignedCapacity;
    if (LOG.isDebugEnabled()) {
      LOG.debug("PlanFollowerEditPolicyTask: total Plan Capacity: {} "
          + "currReservation: {} default-queue capacity: {}", planResources,
          numRes, defQCap);
    }
    try {
      setQueueEntitlement(planQueueName, defReservationQueue, defQCap, 1.0f);
    } catch (YarnException e) {
      LOG.warn(
          "Exception while trying to reclaim default queue capacity for plan: {}",
          planQueueName, e);
    }
    // 第六步: 清理过期的预订记录(now-endTime>24h)
    // garbage collect finished reservations from plan
    try {
      plan.archiveCompletedReservations(now);
    } catch (PlanningException e) {
      LOG.error("Exception in archiving completed reservations: ", e);
    }
    LOG.info("Finished iteration of plan follower edit policy for plan: "
        + planQueueName);

    // Extension: update plan with app states,
    // useful to support smart replanning
  }
```

先看 第一步: 若正在生效的预约资源量大于队列容量，则删除晚接受的部分预约(accept_time)  
可用于兼容集群/队列资源骤减的情况，策略是删除最晚接受的部分预约。  
[SimpleCapacityReplanner.java] 

```
public void plan(Plan plan, List<ReservationDefinition> contracts)
      throws PlanningException {
    if (contracts != null) {
      throw new RuntimeException(
          "SimpleCapacityReplanner cannot handle new reservation contracts");
    }
    ResourceCalculator resCalc = plan.getResourceCalculator();
    Resource totCap = plan.getTotalCapacity();
    long now = clock.getTime();
    // lengthOfCheckZone=1h 
    for (long t = now; (t < plan.getLastEndTime() && t < (now + lengthOfCheckZone)); t +=
        plan.getStep()) {
        // 使用 Plan 所有预订的总资源量 - Plan 的 capacity 来检查是否超限
      Resource excessCap =
          Resources.subtract(plan.getTotalCommittedResources(t), totCap);
      if (Resources.greaterThan(resCalc, totCap, excessCap, ZERO_RESOURCE)) {
         // 按照 ReservationAllocation.acceptedAt 做比较，最新接受的在前
        Set<ReservationAllocation> curReservations =
            new TreeSet<ReservationAllocation>(plan.getReservationsAtTime(t));
        for (Iterator<ReservationAllocation> resIter =
            curReservations.iterator(); resIter.hasNext()
            && Resources.greaterThan(resCalc, totCap, excessCap, ZERO_RESOURCE);) {
          ReservationAllocation reservation = resIter.next();
          // 使用 deleteReservation 来调用 Plan.removeReservation 来删除预订。
          // removeReservation 的源码分析 见"step 3"
          plan.deleteReservation(reservation.getReservationId());
          excessCap =
              Resources.subtract(excessCap, reservation.getResourcesAtTime(t));
          LOG.info("Removing reservation " + reservation.getReservationId()
              + " to repair physical-resource constraints in the plan: "
              + plan.getQueueName());
        }
      }
    }
  }
```

第二步略，第三步: 清除过期预约。  
[AbstractSchedulerPlanFollower.java]  

```
protected void cleanupExpiredQueues(String planQueueName,
      boolean shouldMove, Set<String> toRemove, String defReservationQueue) {
    for (String expiredReservationId : toRemove) {
      try {
        String expiredReservation = getReservationQueueName(planQueueName,
            expiredReservationId);
         // 将过期预约对应的 ReservationQueue 的 capacity 和 maxCapacity 设置为 0 
        setQueueEntitlement(planQueueName, expiredReservation, 0.0f, 0.0f);
        // 默认配置为 true。将其中的 APP 移动到同级的 default 队列中(xxx-default)。
        if (shouldMove) {
          moveAppsInQueueSync(expiredReservation, defReservationQueue);
        }
        if (scheduler.getAppsInQueue(expiredReservation).size() > 0) {
        // 移走所有已有 APP 的情况,还有新提交的。暂时保留
          scheduler.killAllAppsInQueue(expiredReservation);
          LOG.info("Killing applications in queue: {}", expiredReservation);
        } else {
        // 删除队列  
          scheduler.removeQueue(expiredReservation);
          LOG.info("Queue: " + expiredReservation + " removed");
        }
      } catch (YarnException e) {
        LOG.warn("Exception while trying to expire reservation: {}",
            expiredReservationId, e);
      }
    }
  }
```
第四步 先调用 sortByDelta 方法使用 ReservationAllocationComparator 对所有正在生效的预订对应的 ReservationQueue 做排序，然后对所有预订通过 setQueueEntitlement 设置或修正 capacity 和 maxCapacity。   
第五步 通过 setQueueEntitlement 设置 default queue 的 capacity 和 maxCapacity，占有所有剩余资源  
[ReservationAllocationComparator.java]  

```
    @Override
    public int compare(ReservationAllocation lhs, ReservationAllocation rhs) {
      Resource lhsRes = getUnallocatedReservedResources(lhs);
      Resource rhsRes = getUnallocatedReservedResources(rhs);
      // 两个 Resource 先比内存再比虚拟核，小的在前。
      return lhsRes.compareTo(rhsRes);
    }
    
private Resource getUnallocatedReservedResources(
        ReservationAllocation reservation) {
      Resource resResource;
      // 查询常规调度器，获取现有 capacity
      Resource reservationResource = planFollower
          .getReservationQueueResourceIfExists
              (plan, reservation.getReservationId());
      if (reservationResource != null) {
        resResource =
            Resources.subtract(
            // 当前时刻需要的 capacity。可能为负
                reservation.getResourcesAtTime(now),
                reservationResource);
      } else {
        resResource = reservation.getResourcesAtTime(now);
      }
      return resResource;
    }
```

[AbstractSchedulerPlanFollower.java]  

```
protected void setQueueEntitlement(String planQueueName, String currResId,
      float targetCapacity,
      float maxCapacity) throws YarnException {
    String reservationQueueName = getReservationQueueName(planQueueName,
        currResId);
    scheduler.setEntitlement(reservationQueueName, new QueueEntitlement(
        targetCapacity, maxCapacity));
  }
```
[CapacityScheduler.java]

```
public synchronized void setEntitlement(String inQueue,
      QueueEntitlement entitlement) throws SchedulerDynamicEditException,
      YarnException {
    LeafQueue queue = getAndCheckLeafQueue(inQueue);
    ParentQueue parent = (ParentQueue) queue.getParent();
    if (!(queue instanceof ReservationQueue)) {
      throw new SchedulerDynamicEditException("Entitlement can not be"
          + " modified dynamically since queue " + inQueue
          + " is not a ReservationQueue");
    }
    if (!(parent instanceof PlanQueue)) {
      throw new SchedulerDynamicEditException("The parent of ReservationQueue "
          + inQueue + " must be an PlanQueue");
    }
    ReservationQueue newQueue = (ReservationQueue) queue;
    float sumChilds = ((PlanQueue) parent).sumOfChildCapacities();
    // 计算修改后, 所有同级队列的容量之和
    float newChildCap = sumChilds - queue.getCapacity() + entitlement.getCapacity();

    if (newChildCap >= 0 && newChildCap < 1.0f + CSQueueUtils.EPSILON) {
      // 设置前后的 capacity 和 maxCapacity 都一样则 不设置
      if (Math.abs(entitlement.getCapacity() - queue.getCapacity()) == 0
          && Math.abs(entitlement.getMaxCapacity() - queue.getMaximumCapacity()) == 0) {
        return;
      }
      //设置 ReservationQueue 的 capacity 和 maxCapacity 
      newQueue.setEntitlement(entitlement);
    } else {
      throw new SchedulerDynamicEditException(
          "Sum of child queues would exceed 100% for PlanQueue: "
              + parent.getQueueName());
    }
    LOG.info("Set entitlement for ReservationQueue " + inQueue + "  to "
        + queue.getCapacity() + " request was (" + entitlement.getCapacity() + ")");
  }
```
[ReservationQueue.java]

```
public synchronized void setEntitlement(QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {
    float capacity = entitlement.getCapacity();
    if (capacity < 0 || capacity > 1.0f) {
      throw new SchedulerDynamicEditException(
          "Capacity demand is not in the [0,1] range: " + capacity);
    }
    setCapacity(capacity);
    setAbsoluteCapacity(getParent().getAbsoluteCapacity() * getCapacity());
    setMaxCapacity(entitlement.getMaxCapacity());
    if (LOG.isDebugEnabled()) {
      LOG.debug("successfully changed to " + capacity + " for queue "
          + this.getQueueName());
    }
  }
```

第六步: 清理过期的预订记录  
[InMemoryPlan.java]

```
public void archiveCompletedReservations(long tick) {
    LOG.debug("Running archival at time: {}", tick);
    List<InMemoryReservationAllocation> expiredReservations =
        new ArrayList<InMemoryReservationAllocation>();
    readLock.lock();
    try {
    // 删除过期的保留记录  
      long archivalTime = tick - policy.getValidWindow();
      ReservationInterval searchInterval =
          new ReservationInterval(archivalTime, archivalTime);
      SortedMap<ReservationInterval, Set<InMemoryReservationAllocation>> reservations =
          currentReservations.headMap(searchInterval, true);
      if (!reservations.isEmpty()) {
        for (Set<InMemoryReservationAllocation> reservationEntries : reservations
            .values()) {
          for (InMemoryReservationAllocation reservation : reservationEntries) {
            // 结束时间早于当前时间24h 
            if (reservation.getEndTime() <= archivalTime) {
              expiredReservations.add(reservation);
            }
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    if (expiredReservations.isEmpty()) {
      return;
    }
    writeLock.lock();
    try {
      for (InMemoryReservationAllocation expiredReservation : expiredReservations) {
      // Plan.removeReservation 源码分析见"step 3",主要是内存数据结构的维护
        removeReservation(expiredReservation);
      }
    } finally {
      writeLock.unlock();
    }
  }
```
　　　　
### step 6

用户可以在(多个)应用程序的 ApplicationSubmissionContext 中指定 ReservationId 提交到可预订的队列(PlanQueue,具有 reservable 属性的 LeafQueue)   

参见 ApplicationSubmissionContext 的数据结构  

### step 7

常规调度器将从创建的特殊队列中提供容器,以确保遵守资源预定。在预订的时间和资源限制下，用户的(多个)应用程序可以以容量/公平的方式共享资源   

```
public void handle(SchedulerEvent event) {
    switch(event.getType()) {
    ......
    case APP_ADDED:
    {
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      // 解析 APP 提交的队列
      String queueName =
          resolveReservationQueueName(appAddedEvent.getQueue(),
              appAddedEvent.getApplicationId(),
              appAddedEvent.getReservationID());
      if (queueName != null) {
        if (!appAddedEvent.getIsAppRecovering()) {
          addApplication(appAddedEvent.getApplicationId(), queueName,
              appAddedEvent.getUser());
        } else {
          addApplicationOnRecovery(appAddedEvent.getApplicationId(), queueName,
              appAddedEvent.getUser());
        }
      }
    }
    break;
    ......
    }
    }
    
private synchronized String resolveReservationQueueName(String queueName,
      ApplicationId applicationId, ReservationId reservationID) {
    CSQueue queue = getQueue(queueName);
    if ((queue == null) || !(queue instanceof PlanQueue)) {
      return queueName;
    }
    if (reservationID != null) {
      String resQName = reservationID.toString();
      // 根据 reservationId 获取对应的叶子队列
      queue = getQueue(resQName);
      if (queue == null) {
        String message =
            "Application "
                + applicationId
                + " submitted to a reservation which is not yet currently active: "
                + resQName;
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppEvent(applicationId,
                RMAppEventType.APP_REJECTED, message));
        return null;
      }
      if (!queue.getParent().getQueueName().equals(queueName)) {
        String message =
            "Application: " + applicationId + " submitted to a reservation "
                + resQName + " which does not belong to the specified queue: "
                + queueName;
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppEvent(applicationId,
                RMAppEventType.APP_REJECTED, message));
        return null;
      }
     // 使用 ReservationId 对应的 ReservationQueue 来运行任务, 调度容器....
      queueName = resQName;
    } else {
      // 如果指定了 PlanQueue 但是没指定 reservationId, 会提交到 PlanQueue 的 default 队列执行
      queueName = queueName + ReservationConstants.DEFAULT_QUEUE_SUFFIX;
    }
    return queueName;
  }
```

### step 8

预订系统可以兼容容量下降的情况。包括拒绝之前接受最晚的预订兼容 reservable queue 的容量骤减，移动预订到 reservable queue 下的 default队列来兼容超时(预订到期但app 没结束)应用     
参考"step 5"中 AbstractSchedulerPlanFollower#synchronizePlan 方法中对 plan.getReplanner().plan(plan, null) 的调用和cleanupExpiredQueues(planQueueName, plan.getMoveOnExpiry(), expired,
        defReservationQueue)的逻辑  
        
官网解释可能不实，原文如下:  
The system includes mechanisms to adapt to drop in cluster capacity. This consists in replanning by “moving” the reservation if possible, or rejecting the smallest amount of previously accepted reservation (to ensure that other reservation will receive their full amount).

但在 hadoop-2.7.3 和 hadoop-3.2.0的 trunk 分支上 AbstractSchedulerPlanFollower/Planner(只有 SimpleReplanner 实现类)逻辑没有改动，且无相应逻辑  


## QA
>
1. Q: reservable queue 和 leaf queue 区别   
    A: 其一，只有 leaf queue 才能被设置为 reservable queue，通过对 leaf queue 设置 yarn.scheduler.capacity.\<queue-path\>.reservable=true。其二，在资源管理的层级体系中 reservable queue 对应的数据结构是 PlanQueue extend ParentQueue。即实际上，reservable queue 是在 leaf queue上做了配置的 ParentQueue。  
2. Q: PlanQueue 和 ReservationQueue 区别  
    A: PlanQueue 对应一个 reservable queue，ReservationQueue 对应一个 ReservationId，PlanQueue 下面可以创建任意个 ReservationQueue; PlanQueue 是 ParentQueue，ReservationQueue 是 LeafQueue。      
3. Q: 在名称为 X 的 reservable queue 上预订了资源，reservationId=reservationId_001,提交APP 时 Queue 名字填什么？  
    A: Queue 名字填 X,且需要在 ApplicationSubmissionContext 中设置 reservationId。常规调度器会根据 reservationId 解析出对应的 ReservationQueue(名称与 ReservationId 相同)。  
4. Q: 在1:00-2:00 预约了<100G,10core>的资源 30 分钟，但是在 1:00前或者 2:00后提交任务 会怎样?   
   A:  常规调度器会拒绝任务(APP_REJECT)。因为找不到 reservationId 对应的 ReservationQueue,早于1:00时队列还没创建,晚于2:00时队列被删除。最好在1:00提交，可能会有等待时间(RS 调度时从deadline 向 arrival 尝试分配)       
5. Q: 任务运行在哪里?  
   A: 任务运行在与 reservationId 同名的 ReservationQueue 中,如果预约时间过去但是 APP 还没运行完成则默认移动 APP 到与 ReservationQueue 同级的 default 队列中  
6. Q: 能不能把 reservable queue 当做 leaf queue 用? 每个任务都不指定 ReservationId。  
    A: 可以。提交到 reservable queue 但是不指定 ReservationId 的任务都会被移动到 reservable queue 下的 default 队列运行。
7. Q: 提交到 reservable queue 中的 app 有的指定了 ReservationId,有的没指定。运行时有什么影响?  
    A: PlanQueue 优先为有 ReservationId 队列分配资源,其次再为 default 队列分配资源，即只能使用那些被预订后剩余的资源，在资源紧张时 default 队列无资源或被其他任意 ReservationQueue 抢占。(这是由 ReservationSystem 提供预订的逻辑决定的。1.不预约的资源不在ReservationSystem 管理下,请求预约时 ReservationSystem 会认为自身管理 PlanQueue 所有资源都能预约出去  2.为所有预约分配完资源之后剩下的才归属 default 队列所有)  
8. Q: Spark On Yarn 如何使用 ReservationSystem?  
    A: 分两个阶段。第一,使用 Client-RM 协议提交 ReservationSubmissionRequest,预订资源并保存回执的 ReservationId。第二，在 Spark On Yarn 程序中指定 ReservationId 字段，但是此阶段 Spark On Yarn 提交程序尚不完善需要修改源代码。目前 Spark On Yarn 程序使用 createApplicationSubmissionContext 方法来封装 ApplicationSubmissionContext 时, 没有针对 ReservationId 的逻辑。如果有需求的话，可以通过 SparkConf 来传递 ReservationId 字符串,在createApplicationSubmissionContext做解析设置 。    
 9. Q: ReservationSystem 的适用场景  
     A: 1.资源紧张时需要保证重要生产任务运行,可以使用预订资源的方式 。ReservationSystem 在 PlanQueue 下创建 ReservationQueue，将预订需要的绝对值资源量转化 ReservationQueue 在当前时刻的capacity 和 maxCapacity，将任务在此队列中调度，本质上还是 LeafQueue。绝对资源量转化成百分比资源量擦掉了容器数量和大小，不修改开源抢占调度逻辑 不密集抢占抢占的情况下，不能解决大容器调度的问题。  


## 配置预订系统
目前可以在 yarn-site.xml 中配置开启ReservationSystem,可以在CapacityScheduler 及 FairScheduler中增加对预订的支持，方式是在capacity-scheduler.xml 或者 fair-scheduler.xml中的任何 LeafQueue的"reservable"属性标记为 true，然后该队列的配额就可以用于预订。即使没有预订资源，应用程序仍然可以被提交到该队列上，它们将以"best-effort"的模式运行在 预订资源中运行的作业 剩下的容量中。  

## 参考
https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ReservationSystem.html  
https://www.slideshare.net/Hadoop_Summit/reservations-based-scheduling-if-youre-late-dont-blame-us  
http://gitbook.net/java/util/java_util_treemap.html  


