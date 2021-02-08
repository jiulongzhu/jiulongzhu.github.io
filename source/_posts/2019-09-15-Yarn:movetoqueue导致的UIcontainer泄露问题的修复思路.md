---
layout:     post                                    # 使用的布局（不需要改）
title:      Yarn movetoqueue 导致的UI指标错误的修复思路                             # 标题
subtitle:   Yarn 源码解析              #副标题
date:       2019-09-15                          # 时间
author:     jiulongzhu                                          # 作者
catalog: true                                           # 是否归档
tags:                                                           #标签
    - Yarn 2.7.3    
    - 源码解析
---

## 问题背景 

多个线上运行状态私有云的某些资源队列,在无任何 Application 提交及运行、资源队列完全空闲的情况下, 
Used Capacity、Absolute Used Capacity、Used Resource、Num Containers指标非零异常  

<!-- more -->

>
在Yarn ResourceManager管理界面中,有 scheduler 选项卡,展示了Yarn 当前使用的调度器及各资源队列的信息(YarnUI->scheduler->Application Queue),其中每项指标代表的含义是:  
Queue State: 表示当前队列的状态,有 RUNNING/STOPPED 两种状态  
Used Capacity: 表示当前队列已使用的资源占当前队列总资源的百分比  
Configured Capacity: 表示当前队列的资源占父队列资源的百分比  
Configured Max Capacity: 表示当前队列资源最大能占父队列资源的百分比  
Absolute Used Capacity: 表示当前队列已使用的资源占 root 队列资源(整个集群)的百分比  
Absolute Configured Capacity: 表示当前队列的资源占 root 队列总资源的百分比  
Absolute Configured Max Capacity: 表示当前的队列的资源最大能占 root 队列的百分比  
Used Resources: 表示当前队列已使用的资源总量(资源以内存和虚拟核形态表示,基本调度单位)  
Num Schedulable Applications: 表示当前队列调度的应用个数
Num Non-Schedulable Applications: 表示当前队列没有调度(积压,pending)的应用个数  
Num Containers: 表示当前队列已经启动的 container 个数  
Max Applications: 表示当前队列最大并发调度应用个数  
Max Applications Per User: 表示当前队列对每个用户最大并发调度应用个数  
Max Application Master Resources: 表示所有 Application 的 AM 可使用资源量之和的最大值    
Used Application Master Resources: 表示当前队列中所有 Application 的 AM 使用资源量之和  
Max Application Master Resources Per User: 表示当前队列中每个用户的 Application 的 AM 使用资源量之和的最大值  
Configured Minimum User Limit Percent: 表示队列每个用户分配的最低资源百分比(资源保障)  
Configured User Limit Factor: 表示每个用户能占用的队列资源的百分比  
Accessible Node Labels:  表示当前队列可在哪些节点上分配资源 (*为全部节点)  
Preemption: 是否允许资源抢占  

指标可分为两类:配置型指标,静态数据 不会变化,如 Configured Capacity;状态型指标,动态数据 随应用的提交运行结束而变化,如 Used Capacity.  
运行时异常指标如下图,无 Application 运行的情况下,low 队列状态型指标为负
	
![](/img/pictures/negative/negative_e7a766275896.png)


## 先期判断

### 指标关联的变量定位

线上 hadoop 版本: hadoop 2.7.3  
Yarn ResourceManager 管理界面启动的入口是org.apache.hadoop.yarn.server.resourcemanager.ResourceManager#startWebApp()  
[ResourceManager.java]

```
protected void startWepApp() {
	....
    Builder<ApplicationMasterService> builder = 
        WebApps
            .$for("cluster", ApplicationMasterService.class, masterService,
                "ws")
            .with(conf)
            .withHttpSpnegoPrincipalKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY)
            .withHttpSpnegoKeytabKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
            .at(webAppAddress);
   	....
    webApp = builder.start(new RMWebApp(this));
  }
```

RMWebApp 主要逻辑是使用 Google Guice 做依赖注入,并分发请求绑定后台逻辑,大致相当于 SpringMVC 系统中的 Dispatcher 的角色  
对 Yarn UI界面左侧边栏 scheduler 的请求会转发给 RmController#scheduler()方法处理  
[RMWebApp.java]

```
public void setup() {
    ...
    bind(RMWebApp.class).toInstance(this);
    if (rm != null) {
      bind(ResourceManager.class).toInstance(rm);
      bind(ApplicationBaseProtocol.class).toInstance(rm.getClientRMService());
    }
    ...
    route("/scheduler", RmController.class, "scheduler");
    route(pajoin("/queue", QUEUE_NAME), RmController.class, "queue");
    ...
  }
```

RmController#scheduler() 先获取到 Guice 注入的 ResourceManager,然后依据 RM 使用的调度器做页面渲染.  
页面渲染逻辑的入口是CapacitySchedulerPage#render()    
[RmController.java]  

```
public void scheduler() {
    ...
    ResourceManager rm = getInstance(ResourceManager.class);
    ResourceScheduler rs = rm.getResourceScheduler();
    if (rs == null || rs instanceof CapacityScheduler) {
      setTitle("Capacity Scheduler");
      //渲染
      render(CapacitySchedulerPage.class);
      return;
    }
    if (rs instanceof FairScheduler) {
      setTitle("Fair Scheduler");
      render(FairSchedulerPage.class);
      return;
    }
    ....
  }
 protected void render(Class<? extends View> cls) {
    context().rendered = true;
    getInstance(cls).render();
  }
```

泛型上界是 View,调用栈是:  
View#render()  
&ensp;&ensp;->HtmlPage#render()  
&ensp;&ensp;&ensp;&ensp;&ensp;->TwoColumnLayout#render(html)  
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;->CapacitySchedulerPage#content()  
[CapacitySchedulerPage.java] 

```
protected Class<? extends SubView> content() {
    return QueuesBlock.class;
  }
```

QueuesBlock 是 CapacitySchedulerPage 的内部类,用于展示CapacityScheduler 的 DashBoard 信息,包含队列、标签、应用概览信息,并构建根队列 root 来做为后续递归渲染的起点    
[QueuesBlock.java]

```
class QueuesBlock extends HtmlBlock {
    final CapacityScheduler cs;
    final CSQInfo csqinfo;
    private List<NodeLabel> nodeLabelsInfo;
    
    public void render(Block html) {
        ...
        float used = 0;
        if (null == nodeLabelsInfo
            || (nodeLabelsInfo.size() == 1 && nodeLabelsInfo.get(0)
                .getLabelName().isEmpty())) {
                //创建根队列,作为后续渲染的起点
          CSQueue root = cs.getRootQueue();
          CapacitySchedulerInfo sinfo =
              new CapacitySchedulerInfo(root, new NodeLabel(
                  RMNodeLabelsManager.NO_LABEL));
          csqinfo.csinfo = sinfo;
          csqinfo.qinfo = null;

           ...
          ul.li().
            ...
            _(QueueBlock.class)._();
        } else {
          for (NodeLabel label : nodeLabelsInfo) {
            ....
            underLabel.li().
            ...
            _(QueueBlock.class)._()._();
          }
        }
      }
      //Application List 界面
      ul._()._().
      script().$type("text/javascript").
          _("$('#cs').hide();")._()._().
      _(RMAppsBlock.class);
    }
  }
```

从 rootQueue 开始递归,Queue 检查自身有没有子队列 subQueues,若无则其本身为 LeafQueue,使用 LeafQueueInfoBlock#render()渲染叶子队列信息,使用 QueueUsersInfoBlock#render()渲染队列下的用户信息;若有子队列则其本身为ParentQueue,使用 QueueBlock#render()渲染,直至叶子队列  
[QueuesBlock.java]

```
public void render(Block html) {
      ArrayList<CapacitySchedulerQueueInfo> subQueues =
          (csqinfo.qinfo == null) ? csqinfo.csinfo.getQueues().getQueueInfoList()
              : csqinfo.qinfo.getQueues().getQueueInfoList();
      UL<Hamlet> ul = html.ul("#pq");
      for (CapacitySchedulerQueueInfo info : subQueues) {
  	  ...
          if (info.getQueues() == null) {
          li.ul("#lq").li()._(LeafQueueInfoBlock.class)._()._();
          li.ul("#lq").li()._(QueueUsersInfoBlock.class)._()._();
        } else {
          li._(QueueBlock.class);
        }
  	...
      }
    }
```

由于异常指标位于叶子队列信息中,所以暂且不看QueueUsersInfoBlock.java  
[LeafQueueInfoBlock.java]

```
private String nodeLabel;
final CapacitySchedulerLeafQueueInfo lqinfo;
@Inject LeafQueueInfoBlock(ViewContext ctx, CSQInfo info) {
      super(ctx);
      lqinfo = (CapacitySchedulerLeafQueueInfo) info.qinfo;
      nodeLabel = info.label;
    }
protected void render(Block html) {
      if (nodeLabel == null) {
        renderLeafQueueInfoWithoutParition(html);
      } else {
        renderLeafQueueInfoWithPartition(html);
      }
    }

虽然按照 nodeLabel 做了分支,但是核心逻辑都是
renderQueueCapacityInfo方法和renderCommonLeafQueueInfo方法,
这两个方法分别展示不同方面的指标,和 Yarn UI 展示的指标相同 

private void renderQueueCapacityInfo(final ResponseInfo ri) {
      ri.
      //异常指标
      _("Used Capacity:", percent(lqinfo.getUsedCapacity() / 100)).
      _("Configured Capacity:", percent(lqinfo.getCapacity() / 100)).
      _("Configured Max Capacity:", percent(lqinfo.getMaxCapacity() / 100)).
      //异常指标
      _("Absolute Used Capacity:", percent(lqinfo.getAbsoluteUsedCapacity() / 100)).
      _("Absolute Configured Capacity:", percent(lqinfo.getAbsoluteCapacity() / 100)).
      _("Absolute Configured Max Capacity:", percent(lqinfo.getAbsoluteMaxCapacity() / 100)).
      //异常指标
      _("Used Resources:", lqinfo.getResourcesUsed().toString());
    }
 
  private void renderCommonLeafQueueInfo(final ResponseInfo ri) {
      ri.
      _("Num Schedulable Applications:", Integer.toString(lqinfo.getNumActiveApplications())).
      _("Num Non-Schedulable Applications:", Integer.toString(lqinfo.getNumPendingApplications())).
      //异常指标
      _("Num Containers:", Integer.toString(lqinfo.getNumContainers())).
      _("Max Applications:", Integer.toString(lqinfo.getMaxApplications())).
      _("Max Applications Per User:", Integer.toString(lqinfo.getMaxApplicationsPerUser())).
      _("Max Application Master Resources:", lqinfo.getAMResourceLimit().toString()).
      _("Used Application Master Resources:", lqinfo.getUsedAMResource().toString()).
      _("Max Application Master Resources Per User:", lqinfo.getUserAMResourceLimit().toString()).
      _("Configured Minimum User Limit Percent:", Integer.toString(lqinfo.getUserLimit()) + "%").
      _("Configured User Limit Factor:", StringUtils.format(
          "%.1f", lqinfo.getUserLimitFactor())).
      _("Accessible Node Labels:", StringUtils.join(",", lqinfo.getNodeLabels())).
      _("Preemption:", lqinfo.getPreemptionDisabled() ? "disabled" : "enabled");
    }
```

### 异常指标计算方式

注:集群没有使用 label 系统,所以下述的 nodeLabel 视为""即可

* Used Capacity

	[AbstractCSQueue.java]
	
	```
public final synchronized float getUsedCapacity(final String nodeLabel) {
    //集群所有资源 * 该队列的绝对容量百分比 = 该队列的绝对容量 
    Resource availableToQueue =
        Resources.multiply(
            labelManager.getResourceByLabel(nodeLabel, this.clusterResource),
            queueCapacities.getAbsoluteCapacity(nodeLabel));
   //使用 queueUsage 中记录的使用量除以该队列的绝对容量得到队列的 Used Capacity
   //queueUsage 的类型为 ResourceUsage
    return
        Resources.divide(resourceCalculator, this.clusterResource,
            queueUsage.getUsed(nodeLabel), availableToQueue);
  }
	```
* Absolute Used Capacity
	
	算法和Used Capacity算法相似,只是分母不同,AbsoluteUsedCapacity 计算时分母是整个集群的资源  
	
	[AbstractCSQueue.java]
	
	```
	public final synchronized float getAbsoluteUsedCapacity(final String nodeLabel) {
    Resource labeledResources =
               labelManager.getResourceByLabel(nodeLabel, this.clusterResource);
    return Resources.divide(resourceCalculator, this.clusterResource,
        queueUsage.getUsed(nodeLabel), labeledResources);
  }
	```
	
* Used Resource

	&ensp;&ensp;使用的是Queue 按 Label 记录的资源信息  
	queueResourceUsage.getUsed(nodeLabel)  
	
* Num Containers

	&ensp;&ensp;使用的 Queue 本身记录的信息  
	numContainers = leafQueue.getNumContainers();  
	
>
综上所述:  	
四个指标中,队列层面的Used Capacity 、Absolute Used Capacity 、Used Resource 均和 ResourceUsage维护 used 资源信息有关;numContainer 是 LeafQueue 自身维护的 container 数量.这些指标的变化逻辑在 AbstractCSQueue的 assignContainer()和 releaseContainer()中,即和 container 的释放/申请有关.
而在队列完全空闲的时候,四个指标为负,初步定位可能是 Container 重复释放或无效释放的原因.

## 问题复现

### 猜测重复释放

在已经出现指标异常的私有云上,对出现异常时日期前后的 resourcemanager 日志进行分析,检测其中出现"Assigned container"和"Released  container"字符串的数量,从日志层面对 container 的申请和释放次数做一个简单的判断.这种方式适用于集群作业周期性较强且没有跨天任务运行的情况.  

```
egrep -o "Assigned container" resourcemanager.log | sort | uniq -c
egrep -o "Released container" resourcemanager.log | sort | uniq -c 
```

从结果上看,"Assigned container"和"Released container"字符串出现次数是一致的,可能不是重复释放的问题  

### 猜测无效释放	

对无效释放的猜想源自于重复释放和 rm 日志中的"Null container completed...",猜测可能对于 nullContainer 处理有问题,修改了系统维护的信息, 导致了负值    

UI 的 kill 操作,命令行 yarn application --kill 和 RMApp 的正常结束都会释放 container,由 APP_ATTEMPT_REMOVED 事件触发,
回收 AppAttemp 持有的runningContainer 和 reservedContainer,核心逻辑在 CapacityScheduler#completedContainer()方法,但是其中对于 RMContainer 和 Application 都做了校验,不会修改维护信息;如果 RMContainer 对象内持有的 contianer 对象为空的话,会抛出 NullPointerException,也不会修改维护信息. 所以无效释放的猜测不合理,此处代码证明重复释放也不合理
[CapacityScheduler.java]

```
protected synchronized void completedContainer(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) { 
    //rmContaienr 空值校验  
    if (rmContainer == null) {
      LOG.info("Null container completed...");
      return;
    }
    Container container = rmContainer.getContainer();
    FiCaSchedulerApp application =
        getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
    //如果 RMContainer 映射的 container 为空,则此处会 NullPointerException
        container.getId().getApplicationAttemptId().getApplicationId();
    //application 空值校验
    if (application == null) {
      LOG.info("Container " + container + " of" + " unknown application "
          + appId + " completed with event " + event);
      return;
    }
    ....
  }
```

### movetoqueue

重复释放和无效释放的猜测证否之后,只好再去仔细研究 系统指标异常前的几个运行的任务日志,对 application 的 attempt 和每个 contianer 状态机的状态和 触发事件按照时间线标注出来,对application 发生的所有事件还原出来,发现了其中一个 application 的以下信息  
>  
1.提交到 root.high 队列后,队列资源不足,在某些节点上为该 applicaiton reserve 资源,其他 application 调度时不会再该节点上分配资源  
2.application 被从 high 队列移动了 low 队列  
3.container 被移动队列时,源队列和目标队列的资源有变动,两个队列都会被 re-sort,以便优先在资源利用率最低的 queue 调度,所以有每个 container 的移动有四条日志:queueMoveOut 队列的信息,queueMoveOut 的父队列信息(有几个 parentQueue 就会有几条日志),queuMoveIn 队列信息,queueMoveIn 的父队列信息  
4.在 move container 前后有一个 container: container_e08_1565789460020_5864_01_000002,在 high队列上分配了资源,被 move 到 low 队列后,containerCompleted 之后,释放资源到了 low 队列  
5.<b>在 move container 前后有一个 container: container_e08_1565789460020_5864_01_000080,在 high 队列上保留了资源,move 操作没有影响到该 container,但是containerCompleted之后释放资源到了 low 队列</b>. 

日志如下  

```
INFO LeafQueue: assignedContainer application attempt=appattempt_1565789460020_5864_000001 container=Container: [ContainerId: container_e08_1565789460020_5864_01_000002, NodeId: hadoop6:8041, NodeHttpAddress: hadoop6:8042, Resource: <memory:11264, vCores:1>, Priority: 1, Token: null, ] queue=high: capacity=0.6, absoluteCapacity=0.6, usedResources=<memory:420864, vCores:20>, usedCapacity=1.4421053, absoluteUsedCapacity=0.86526316, numApps=3, numContainers=20 clusterResource=<memory:486400, vCores:136> type=OFF_SWITCH  
INFO RMContainerImpl: container_e08_1565789460020_5864_01_000080 Container Transitioned from NEW to RESERVED
INFO LeafQueue: Reserved container  application=application_1565789460020_5864 resource=<memory:11264, vCores:1> queue=high: capacity=0.6, absoluteCapacity=0.6, usedResources=<memory:443392, vCores:22>, usedCapacity=1.5192982, absoluteUsedCapacity=0.91157895, numApps=3, numContainers=22 usedCapacity=1.5192982 absoluteUsedCapacity=0.91157895 used=<memory:443392, vCores:22> cluster=<memory:486400, vCores:136>
INFO ParentQueue: Re-sorting assigned queue: root.high stats: high: capacity=0.6, absoluteCapacity=0.6, usedResources=<memory:454656, vCores:23>, usedCapacity=1.5578947, absoluteUsedCapacity=0.93473685, numApps=3, numContainers=23
....moving
INFO LeafQueue: movedContainer container=Container: [ContainerId: container_e08_1565789460020_5864_01_000002, NodeId: hadoop6:8041, NodeHttpAddress: hadoop6:8042, Resource: <memory:11264, vCores:1>, Priority: 1, Token: Token { kind: ContainerToken, service:  }, ] resource=<memory:11264, vCores:1> queueMoveOut=high: capacity=0.6, absoluteCapacity=0.6, usedResources=<memory:431104, vCores:20>, usedCapacity=1.477193, absoluteUsedCapacity=0.88631576, numApps=3, numContainers=20 usedCapacity=1.477193 absoluteUsedCapacity=0.88631576 used=<memory:431104, vCores:20> cluster=<memory:486400, vCores:136>
INFO ParentQueue: movedContainer queueMoveOut=root usedCapacity=0.9768421 absoluteUsedCapacity=0.9768421 used=<memory:475136, vCores:24> cluster=<memory:486400, vCores:136>
INFO LeafQueue: movedContainer container=Container: [ContainerId: container_e08_1565789460020_5864_01_000002, NodeId: hadoop6:8041, NodeHttpAddress: hadoop6:8042, Resource: <memory:11264, vCores:1>, Priority: 1, Token: Token { kind: ContainerToken, service: }, ] resource=<memory:11264, vCores:1> queueMoveIn=low: capacity=0.1, absoluteCapacity=0.1, usedResources=<memory:-22528, vCores:-3>, usedCapacity=-0.4631579, absoluteUsedCapacity=-0.04631579, numApps=0, numContainers=-3 usedCapacity=-0.4631579 absoluteUsedCapacity=-0.04631579 used=<memory:-22528, vCores:-3> cluster=<memory:486400, vCores:136>
INFO ParentQueue: movedContainer queueMoveIn=root usedCapacity=1.0 absoluteUsedCapacity=1.0 used=<memory:486400, vCores:25> cluster=<memory:486400, vCores:136>
....moved
INFO CapacityScheduler: App: application_1565789460020_5864 successfully moved from high to: low
INFO LeafQueue: completedContainer container=Container: [ContainerId: container_e08_1565789460020_5864_01_000080, NodeId: hadoop5:8041, NodeHttpAddress: hadoop5:8042, Resource: <memory:11264, vCores:1>, Priority: 1, Token: null, ] queue=low: capacity=0.1, absoluteCapacity=0.1, usedResources=<memory:0, vCores:-1>, usedCapacity=0.0, absoluteUsedCapacity=0.0, numApps=1, numContainers=-1 cluster=<memory:486400, vCores:136>
INFO LeafQueue: completedContainer container=Container: [ContainerId: container_e08_1565789460020_5864_01_000002, NodeId: hadoop6.:8041, NodeHttpAddress: hadoop6.cn:8042, Resource: <memory:11264, vCores:1>, Priority: 1, Token: Token { kind: ContainerToken, service: }, ] queue=low: capacity=0.1, absoluteCapacity=0.1, usedResources=<memory:-1024, vCores:-2>, usedCapacity=-0.021052632, absoluteUsedCapacity=-0.002105263, numApps=1, numContainers=-2 cluster=<memory:486400, vCores:136>
```

>猜测:application 在移动队列后,对 reservedContainer 没有移动或维护信息不同步,导致了源队列的资源泄露给了目标队列,从而目标队列的 UsedCapacity 为负  
>复现方式  
&ensp;&ensp;&ensp;&ensp;第一步:提交 application 到资源紧张的 Queue  
&ensp;&ensp;&ensp;&ensp;第二步:待在 RM 的日志中看到"Trying to fulfill reservation for application ${APPLICATION_ID} on node ..."和"Trying to schedule on node..., available:..." 表明 CapacityScheduler为该 ApplicationId 保留了资源,跳过在此 nm 上为其他 app 分配 container    
&ensp;&ensp;&ensp;&ensp;第三步: 使用 yarn application -movetoqueue ${APPLICATION_ID} -queue ${TOQueue}  
&ensp;&ensp;&ensp;&ensp;第四步: 待在 RM 日志中看到了"App:${APPLICATION_ID} successfully moved ${FROMQUEUE} to ${TOQUEUE}"后,使用 yarn application --kill ${APPLICATION_ID} 或等待 app 结束   
&ensp;&ensp;&ensp;&ensp;第五步: 在 YarnUI上查看被移动的目标队列${TOQUEUE}的信息  

注:  
&ensp;&ensp;&ensp;&ensp;1.一定要在资源紧张的队列上提交 app,以触发调度系统的保留资源.在空闲队列上提交任务复现不了指标异常的问题  
&ensp;&ensp;&ensp;&ensp;2.最好移动到一个完全空闲的队列上,否则即使复现了资源泄露,也不易看出来  

## 源码解析

以下代码出自于 hadoop 2.7.3版本  
Yarn 是一个资源调度平台,集群内存资源和 cpu 资源被 Yarn 抽象为 Resource{memory,core},客户端对 Yarn 的资源请求和 Yarn 内部的资源调度都是以 Container 为基本单位的    
当客户端向资源队列 Queue 提交 Application 时, 客户端申请的 AM 以及 AM 申请新的执行角色(e.g. spark 的 executor)都是在Container 中运行,那么对于 Yarn 调度的 Container,资源信息被多维聚合[app状态|container状态|container用途|用户|标签]统计维护:    

* Queue.包括直接申请的叶子队列及其所有的父队列    
	1. Queue(AbstractCSQueue)    
 &ensp;&ensp;主要指标是 container 数量;保存着 QueueMetrics 和 ResourceUsage(ByLabel) 的引用     
		a. ParentQueue   
&ensp;&ensp;主要指标是运行的 application数量;保存着所有资源子队列的集合    
		b. LeafQueue 
&ensp;&ensp; 主要指标是每个用户提交 app 的数量和资源用途用量(ResourceUsageByUser);保存着队列 running 和 pending 的 app[attemp]信息  	 		
	2. QueueMetrics  
&ensp;&ensp;保留着queue 的指标信息,包括[提交|运行|积压|完成|杀死|失败]app 数量、[分配|待分配|积压]的[container|内存|虚拟核]信息、活跃的[app|user]信息;保存着用户级别的 <username,QueueMetrics>映射  
	3. ResourceUsage			
&ensp;&ensp;保留着<label,UsageByLabel>信息,分为used、pending、amused、reserved 四类  
* Application(SchedulerApplicationAttempt). app 中维护着当前正在运行的 container,及 yarn 为其保留的 container 信息
* NodeManager. container 所在的 NM 维护着自身运行的所有 container,并通过 RM 心跳汇报所有 container 状态机状态,触发 RM 对container 状态的更新或释放    

>
综上:  
&ensp;&ensp;&ensp;&ensp;1.指标异常与 container 申请释放有关,所以需要研究 <b>正常申请释放与应用移动队列</b> 对上述维护信息的影响  
&ensp;&ensp;&ensp;&ensp;2.UI 展示的 numContainers 数据取自 AbstractCSQueue 维护 numContainer 成员变量;usedCapacity,absoluteUsedCapacity,usedResource 均取自AbstractCSQueue$ResourceUsage.used 成员变量.需要关注以上变量在资源申请释放过程中的变化    

### 正常的资源分配和释放过程

RM 的资源分配和资源释放都是被动触发,客户端提交 application 到 RMClientService,app 信息暂存在指定资源队列中,待 NM 向 RM 通过心跳汇报自身信息时 RM 将对该 NM 触发调度,在情况允许的情况下(e.g. 目标队列具有该 NM 的 access 权限,NM 剩余资源满足 app 中一个 ResourceRequest 申请的资源),将在该 NM 上划出 ResourceRequest 要求的资源,并同步 Queue/Application/NodeManager 维护的信息;当 container 完成退出之后,NodeManager 注意到 container 状态机变化,并将其信息附带在心跳中汇报给 RM,触发 Queue/Application/NodeManager 状态变化和信息同步   
以下代码较为关注资源信息的维护同步,对于其他细节不再赘述    
	
### 资源分配

![](/img/pictures/negative/yarn_node_update_pic.png)

NM 通过心跳触发 CapacityScheduler 调度,CS 首先按照 NM 上报的信息同步 RM 的信息,为新启动的 container 触发 LAUNCHED 事件,为结束的 container 触发FINISHED 事件;并试图在该节点上分配资源  
[CapacityScheduler.java]

```
public void handle(SchedulerEvent event) {
...
case NODE_UPDATE:
    {
      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
      RMNode node = nodeUpdatedEvent.getRMNode();
      //同步 RM 信息
      nodeUpdate(node);
      if (!scheduleAsynchronously) {
      //试图在 nm 分配资源
        allocateContainersToNode(getNode(node.getNodeID()));
      }
    }
    break;
 ...
}
```
依据汇报心跳的 NodeManager 是否被保留了资源执行相应的逻辑  
[CapacityScheduler.java]

```
private synchronized void allocateContainersToNode(FiCaSchedulerNode node) {
    ...
    RMContainer reservedContainer = node.getReservedContainer();
    //当前 node 被某 applocation 保留了
    if (reservedContainer != null) {
      FiCaSchedulerApp reservedApplication =
          getCurrentAttemptForContainer(reservedContainer.getContainerId());
      // Try to fulfill the reservation
      LOG.info("Trying to fulfill reservation for application " + 
          reservedApplication.getApplicationId() + " on node: " + 
          node.getNodeID());
      LeafQueue queue = ((LeafQueue)reservedApplication.getQueue());
      CSAssignment assignment =
          queue.assignContainers(
              clusterResource,
              node,
              new ResourceLimits(labelManager.getResourceByLabel(
                  RMNodeLabelsManager.NO_LABEL, clusterResource)));
      ....
      }
    }
    //当前节点未被保留
    if (node.getReservedContainer() == null) {
     	...
     	//交由 root 队列代理分配.root 是资源队列树形结构的根节点,类型一定ParentQueue
        root.assignContainers(
            clusterResource,
            node,
            new ResourceLimits(labelManager.getResourceByLabel(
                RMNodeLabelsManager.NO_LABEL, clusterResource)));
      }
    } else {
      LOG.info("Skipping scheduling since node " + node.getNodeID() + 
          " is reserved by application " + 
          node.getReservedContainer().getContainerId().getApplicationAttemptId()
          );
    }
  
  }
```
ParentQueue 将节点委派给子队列,试图分配资源  
[ParentQueue.java]

```
  public synchronized CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits resourceLimits) {
    CSAssignment assignment = 
        new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
    Set<String> nodeLabels = node.getLabels();
    ...校验 queue 对 node 的 access 权限
    while (canAssign(clusterResource, node)) {
      ...校验 queue 资源是否超限      
      // Schedule 递归交由子队列去分配
      CSAssignment assignedToChild = 
          assignContainersToChildQueues(clusterResource, node, resourceLimits);
      assignment.setType(assignedToChild.getType());
      // Done if no child-queue assigned anything
      // 如果分配到了资源, assignedToChild 大于 Resource<0,0>
      if (Resources.greaterThan(
              resourceCalculator, clusterResource, 
              assignedToChild.getResource(), Resources.none())) {
              //同步维护的信息
        super.allocateResource(clusterResource, assignedToChild.getResource(),
            nodeLabels);
        Resources.addTo(assignment.getResource(), assignedToChild.getResource());
        ...
         } else {
        break;
      }
      ...
      }
	...
        break;
      }
    }     
    return assignment;
  }
```
先看如果分配到资源的话 同步的信息:  
1.按照 label 去更新 ResourceUsage 中维护的用户使用资源 used(incUsed 方法)    
2.增加了该队列的 numContainer 数量  
需要注意的是 <b>无论后续得到的是 allocated container 还是 reserved container,都增加了 numContainer的值,增加了用户 USED 类型的内存和虚拟核数量</b>   
[AbstractCSQueue.java]

```
synchronized void allocateResource(Resource clusterResource, 
      Resource resource, Set<String> nodeLabels) {
    // Update usedResources by labels
    if (nodeLabels == null || nodeLabels.isEmpty()) {
      queueUsage.incUsed(resource);
    } else {
      Set<String> anls = (accessibleLabels.contains(RMNodeLabelsManager.ANY))
          ? labelManager.getClusterNodeLabels() : accessibleLabels;
      for (String label : Sets.intersection(anls, nodeLabels)) {
        queueUsage.incUsed(label, resource);
      }
    }
    ++numContainers;
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
  }
```
再看对 queueUsage:ResourceUsage 做了什么操作  
ResourceUsage 中维护了一个Map结构 usages,key 是标签类型,value 是 UsageByLabel;UsageByLabel 中只有一个数组 Resource[],数组中的每个值分别表征着 USED,PENDING,AMUSED,RESERVED 用途的 Resource 数量  
[ResourceUsage.java]

```
// <labelName,UsageByLabel>
private Map<String, UsageByLabel> usages;
public void incUsed(String label, Resource res) {
    _inc(label, ResourceType.USED, res);
 }
 private void _inc(String label, ResourceType type, Resource res) {
     ...
     UsageByLabel usage = getAndAddIfMissing(label);
     Resources.addTo(usage.resArr[type.idx], res);      
     ...
   }
```
UsageByLabel 使用一个 Resource[]数组来存储资源的用途和用量  
[UsageByLabel.java]  

```
  private static class UsageByLabel {
    // usage by label, contains all UsageType
    private Resource[] resArr;
    public UsageByLabel(String label) {
      resArr = new Resource[ResourceType.values().length];
      for (int i = 0; i < resArr.length; i++) {
        resArr[i] = Resource.newInstance(0, 0);
      };
    }
  }
  
   private enum ResourceType {
    USED(0), PENDING(1), AMUSED(2), RESERVED(3);
    private int idx;
    private ResourceType(int value) {
      this.idx = value;
    }
  }
```
再回到资源分配,root 队列深度优先遍历所有子队列,尝试在叶子队列上分配资源  
[LeafQueue.java]

```
public synchronized CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits currentResourceLimits) {
	....
    // Check for reserved resources
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      FiCaSchedulerApp application = 
          getApplication(reservedContainer.getApplicationAttemptId());
      synchronized (application) {
        return assignReservedContainer(application, node, reservedContainer,
            clusterResource);
      }
    }
    //对目前活跃的 application,尝试在当前 NM 上分配资源
    for (FiCaSchedulerApp application : activeApplications) {
    	....
      synchronized (application) {
         ....        
          // Schedule in priority order
        for (Priority priority : application.getPriorities()) {
          ResourceRequest anyRequest =
              application.getResourceRequest(priority, ResourceRequest.ANY);
         if (null == anyRequest) {
            continue;
          }
          ....校验性工作
          //校验通过,在此节点上分配资源
          CSAssignment assignment =  
            assignContainersOnNode(clusterResource, node, application, priority, 
                null, currentResourceLimits);
	.....
          // Did we schedule or reserve a container?
          Resource assigned = assignment.getResource();
          if (Resources.greaterThan(
              resourceCalculator, clusterResource, assigned, Resources.none())) {
            //更新当前[叶子]队列的 numContainer,ResourceUsageByLabel和 ResourceUsageByUser.
            //比 ParentQueue.allocateResource 多出了一个用户层面的资源统计
            allocateResource(clusterResource, application, assigned,
                node.getLabels());
            .....            
            return assignment;
          } else {
            break;
          }
        }
      }
    }
    return NULL_ASSIGNMENT;
  }
```
先看LeafQueue#allocateResource 更新信息时更新的指标和内容
LeafQueue 和ParentQueue  一样,使用抽象父类AbstractCSQueue#allocateResource() 更新当前队列的 numContainer 和 标签层面的 ResourceUsage  
然后使用 LeafQueue 中维护的Map<String,User>成员变量 users来维护用户标签层面的资源信息 
[LeafQueue.java]

```
synchronized void allocateResource(Resource clusterResource,
      SchedulerApplicationAttempt application, Resource resource,
      Set<String> nodeLabels) {
      //使用的是 AbstractCSQueue#allocateResource,和 ParentQueue更新的指标及内容相同:numContainer,ResourceUsageByLabel
    super.allocateResource(clusterResource, resource, nodeLabels);
    //更新用户层面的 ResourceUsage
    String userName = application.getUser();
    User user = getUser(userName);
    user.assignContainer(resource, nodeLabels);
    Resources.subtractFrom(application.getHeadroom(), resource); // headroom
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
  }
```
User 类中有一个 ResourceUsage,维护着在用户层面 各标签的资源用途和用量  
[User.java]

```
public static class User {
    ResourceUsage userResourceUsage = new ResourceUsage();
    volatile Resource userResourceLimit = Resource.newInstance(0, 0);
    int pendingApplications = 0;
    int activeApplications = 0;
     public void assignContainer(Resource resource,
        Set<String> nodeLabels) {
      if (nodeLabels == null || nodeLabels.isEmpty()) {
        userResourceUsage.incUsed(resource);
      } else {
        for (String label : nodeLabels) {
          userResourceUsage.incUsed(label, resource);
        }
      }
    }
  }
```
上文 LeafQueue#assignContainers方法 遍历每个活跃的 application,尝试在当前 nodemanager 上分配资源,调用 assignContainersOnNode()方法进行下一步的分配逻辑,并增加 app 在本地化层面分配的 container 数量,此指标逻辑不在此讨论        
按照本地性优先级,优先分配  NODE_LOCAL(本节点)>RACK_LOCAL(本机架)>OFF_SWITCH(跨机架)  
YARN 的本地性不同于 MR/SPARK 的本地性,YARN的本地性性体现在 Client/AM申请 container 的时候可以指定 container 所在的节点,此处的 NODE_LOCAL、RACK_LOCAL 和 OFF_SWITCH 是相对于申请 container 时指定的节点而言;数据本地化计算其实更多的是靠计算框架配合,按照数据所处的位置优先分配给计算节点(e.g. RDD#getPreferredLocations)    
[LeafQueue.java]

```
private CSAssignment assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, ResourceLimits currentResoureLimits) {
    Resource assigned = Resources.none();
    NodeType requestType = null;
    MutableObject allocatedContainer = new MutableObject();
    // Data-local
    ResourceRequest nodeLocalResourceRequest =
        application.getResourceRequest(priority, node.getNodeName());
    if (nodeLocalResourceRequest != null) {
      requestType = NodeType.NODE_LOCAL;
      assigned =
          assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest, 
            node, application, priority, reservedContainer,
            allocatedContainer, currentResoureLimits);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
          assigned, Resources.none())) {
        //update locality statistics
        if (allocatedContainer.getValue() != null) {
          application.incNumAllocatedContainers(NodeType.NODE_LOCAL,
            requestType);
        }
        return new CSAssignment(assigned, NodeType.NODE_LOCAL);
      }
    }
    // Rack-local
    ResourceRequest rackLocalResourceRequest =
        application.getResourceRequest(priority, node.getRackName());
    if (rackLocalResourceRequest != null) {
      if (!rackLocalResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }
      if (requestType != NodeType.NODE_LOCAL) {
        requestType = NodeType.RACK_LOCAL;
      }
      assigned = 
          assignRackLocalContainers(clusterResource, rackLocalResourceRequest, 
            node, application, priority, reservedContainer,
            allocatedContainer, currentResoureLimits);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
          assigned, Resources.none())) {
        if (allocatedContainer.getValue() != null) {
          application.incNumAllocatedContainers(NodeType.RACK_LOCAL,
            requestType);
        }
        return new CSAssignment(assigned, NodeType.RACK_LOCAL);
      }
    }
    // Off-switch
    ResourceRequest offSwitchResourceRequest =
        application.getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchResourceRequest != null) {
      if (!offSwitchResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }
      if (requestType != NodeType.NODE_LOCAL
          && requestType != NodeType.RACK_LOCAL) {
        requestType = NodeType.OFF_SWITCH;
      }
      assigned =
          assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
            node, application, priority, reservedContainer,
            allocatedContainer, currentResoureLimits);
      if (allocatedContainer.getValue() != null) {
        application.incNumAllocatedContainers(NodeType.OFF_SWITCH, requestType);
      }
      return new CSAssignment(assigned, NodeType.OFF_SWITCH);
    }
    return SKIP_ASSIGNMENT;
  }
```
LeafQueue#assignNodeLocalContainers,assignRackLocalContainers,assignOffSwitchContainers的核心逻辑被封装为一处,只是本地化类型 NodeType 不同    
[LeafQueue.java]

```
private Resource assignContainer(Resource clusterResource, FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority, 
      ResourceRequest request, NodeType type, RMContainer rmContainer,
      MutableObject createdContainer, ResourceLimits currentResoureLimits) {
    ....    
    Resource capability = request.getCapability();
    Resource available = node.getAvailableResource();
    Resource totalResource = node.getTotalResource();
    if (!Resources.lessThanOrEqual(resourceCalculator, clusterResource,
        capability, totalResource)) {
      LOG.warn("Node : " + node.getNodeID()
          + " does not have sufficient resource for request : " + request
          + " node total capability : " + node.getTotalResource());
      return Resources.none();
    }
    // Create the container if necessary
    Container container = 
        getContainer(rmContainer, application, node, capability, priority);
  	...
      boolean shouldAllocOrReserveNewContainer = shouldAllocOrReserveNewContainer(
        application, priority, capability);
    //依据 节点可用资源量与申请的资源量 做除法来判断节点剩余资源能否满足需求 
    int availableContainers = 
        resourceCalculator.computeAvailableContainers(available, capability);
    boolean needToUnreserve = Resources.greaterThan(resourceCalculator,clusterResource,
        currentResoureLimits.getAmountNeededUnreserve(), Resources.none());
	
    if (availableContainers > 0) {
      //如果节点上足够分配一个 container 则分配
      ....
      //调用 application 和 nodemanager 的方法,触发这两处的信息同步
      RMContainer allocatedContainer = 
          application.allocate(type, node, priority, request, container);
          
      node.allocateContainer(allocatedContainer);
      
      createdContainer.setValue(allocatedContainer);
      return container.getResource();
    } else {
    	....
        //节点上没有足够的资源满足需求,则为该 application 保留该节点的资源
        reserve(application, priority, node, rmContainer, container);
        return request.getCapability();
      }
      return Resources.none();
    }
  }
```
上文有三处地方需要同步资源信息:reserve()方法,FiCaSchedulerApp#allocate,FiCaSchedulerNode#allocateContainer  
先看FiCaSchedulerApp#allocate方法,在 application 层面修改了什么信息    
[FiCaSchedulerApp.java]

```
 synchronized public RMContainer allocate(NodeType type, FiCaSchedulerNode node,
      Priority priority, ResourceRequest request, 
      Container container) {
     ....
    //将 container 封装成 RMContainer 的形式,记录在 application 的所有 container 集合 newlyAllocatedContainers 和 运行态(相对 reserved)的 container 集合 liveContainers 中  
    RMContainer rmContainer = new RMContainerImpl(container, this
        .getApplicationAttemptId(), node.getNodeID(),
        appSchedulingInfo.getUser(), this.rmContext);
    newlyAllocatedContainers.add(rmContainer);
    liveContainers.put(container.getId(), rmContainer);    
    ....
    //更新 metrics 信息
    List<ResourceRequest> resourceRequestList = appSchedulingInfo.allocate(
        type, node, priority, request, container);
     //当前 app 消费的资源量
    Resources.addTo(currentConsumption, container.getResource());
    ...
    //触发 RMContainer 状态机变化,container 可以准备运行时环境,下载依赖等
    rmContainer.handle(
        new RMContainerEvent(container.getId(), RMContainerEventType.START));
    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.ALLOC_CONTAINER, "SchedulerApp", 
        getApplicationId(), container.getId());
    return rmContainer;
```
AppSchedulingInfo#allocate 更新 QueueMetrics 信息  
[AppSchedulingInfo.java]

```
synchronized public List<ResourceRequest> allocate(NodeType type,
      SchedulerNode node, Priority priority, ResourceRequest request,
      Container container) {
    List<ResourceRequest> resourceRequests = new ArrayList<ResourceRequest>();
    //本地化层面的统计信息
    if (type == NodeType.NODE_LOCAL) {
      allocateNodeLocal(node, priority, request, container, resourceRequests);
    } else if (type == NodeType.RACK_LOCAL) {
      allocateRackLocal(node, priority, request, container, resourceRequests);
    } else {
      allocateOffSwitch(node, priority, request, container, resourceRequests);
    }
    QueueMetrics metrics = queue.getMetrics();
    ...
    //对QueueMetrics 做已经分配的资源的统计
    metrics.allocateResources(user, 1, request.getCapability(), true);
    metrics.incrNodeTypeAggregations(user, type);
    return resourceRequests;
  }
```

QueueMetrics#allocateResource 对 userMetrics 做判断;对 parent 也做了判断 如果parent 非空那么会递归更新 parent 的信息,对userMetrics 和 parent 执行的方法都是 QueueMetrics#allocateResources,更新的指标一样:增加 allocate 的 container 数量,内存量,虚拟核数量  
[QueueMetrics.java] 

```
public void allocateResources(String user, int containers, Resource res,
      boolean decrPending) {
    allocatedContainers.incr(containers);
    aggregateContainersAllocated.incr(containers);
    allocatedMB.incr(res.getMemory() * containers);
    allocatedVCores.incr(res.getVirtualCores() * containers);
    if (decrPending) {
      _decrPendingResources(containers, res);
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.allocateResources(user, containers, res, decrPending);
    }
    if (parent != null) {
      parent.allocateResources(user, containers, res, decrPending);
    }
  }
```
FiCaSchedulerNode#allocateContainer, NodeManager 同步 allocate 信息 
[FiCaSchedulerNode.java]

```
public synchronized void allocateContainer(RMContainer rmContainer) {
    Container container = rmContainer.getContainer();
     //减少当前 nm 的可用资源,增加当前 nm 的已分配资源
    deductAvailableResource(container.getResource());
    //增加自身维护的 numContainer 数量,不同于 AbstractCSQueue.numContainers,维护Queue维度和 NodeManager 维度的 container 数量
    ++numContainers;
    //启动的 container 列表
    launchedContainers.put(container.getId(), rmContainer);
    ...
  }
  private synchronized void deductAvailableResource(Resource resource) {
    Resources.subtractFrom(availableResource, resource);
    Resources.addTo(usedResource, resource);
  }
```
如果在节点上 reserve 了资源,同步的信息和 allocate 的方式相似,会更新 QueueMetrics 对 reserved[containr|memory|core]的信息,更新application reservedContainer 及 currentReservationMemroy 信息,更新 NodeManager 当前 reservedContainer 信息  
[LeafQueue.java]

```
private void reserve(FiCaSchedulerApp application, Priority priority, 
      FiCaSchedulerNode node, RMContainer rmContainer, Container container) {
    //传入的 rmContainer 为 null
    if (rmContainer == null) {
      getMetrics().reserveResource(
          application.getUser(), container.getResource());
    }
    // Inform the application 
    rmContainer = application.reserve(node, priority, rmContainer, container);
    // Update the node
    node.reserveResource(application, priority, rmContainer);
  }
```
QueueMetrics#reserveResource在下方有一个对 parent 的判断,递归调用依次更新 ParentQueue 的信息;有一个对 userMetrics 的判断,对 QueueMetrics 中维护的 users:Map[String, QueueMetrics] 同步用户层面的统计信息,执行的方法都是当前方法QueueMetrics#reserveResource(),同步reserved 资源信息:增加 reserved container 数量,内存量,虚拟核数量  

[QueueMetrics.java]

```
public void reserveResource(String user, Resource res) {
    reservedContainers.incr();
    reservedMB.incr(res.getMemory());
    reservedVCores.incr(res.getVirtualCores());
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.reserveResource(user, res);
    }
    if (parent != null) {
      parent.reserveResource(user, res);
    }
  }
```
application 同步 reserved container 信息  
[SchedulerApplicationAttempt.java]

```
 public synchronized RMContainer reserve(SchedulerNode node, Priority priority,
      RMContainer rmContainer, Container container) {
     //上游传入的 rmContainer 为 null
    if (rmContainer == null) {
      rmContainer = 
          new RMContainerImpl(container, getApplicationAttemptId(), 
              node.getNodeID(), appSchedulingInfo.getUser(), rmContext);
      //增加当前 application 保留的资源信息
      Resources.addTo(currentReservation, container.getResource());
      resetReReservations(priority);
    } else {
      // Note down the re-reservation
      addReReservation(priority);
    }
    //在 container 信息中设置了绑定节点的信息....
    rmContainer.handle(new RMContainerReservedEvent(container.getId(), 
        container.getResource(), node.getNodeID(), priority));
        //加入到维护的 reservedContainer 信息中
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    if (reservedContainers == null) {
      reservedContainers = new HashMap<NodeId, RMContainer>();
      this.reservedContainers.put(priority, reservedContainers);
    }
    reservedContainers.put(node.getNodeID(), rmContainer);
    return rmContainer;
  }
```
FicaSchedulerNode#reserveResource(),NodeManager 同步 reserved container 信息  
[FicaSchedulerNode.java]

```
public synchronized void reserveResource(
      SchedulerApplicationAttempt application, Priority priority,
      RMContainer container) {
    // Check if it's already reserved
    RMContainer reservedContainer = getReservedContainer();
    ....空值校验性,重复保留校验等操作
    //设置当前节点保留的 container 为传入值
    setReservedContainer(container);
  }

```


> 综上:  
        CS 调度器在 NM 上为 APP 分配 container 时,会同步 [Parent | Leaf] Queue/APP/NM 三个位置维护的信息    
	1.分配时,无论从叶子队列得到 allocated 或 reserved 类型的 container, 其 <b>ParentQueue</b>都会维护:  
&ensp;&ensp;&ensp;&ensp;ParentQueue 维护的 numContainer  
&ensp;&ensp;&ensp;&ensp;ParentQueue <b>标签维度</b>的 QueueUsage 中 USED 用途的资源量  
	2.分配时,无论从叶子队列得到 allocated 或 reserved 类型的 container, <b>LeafQueue</b> 本身都会维护:  
&ensp;&ensp;&ensp;&ensp; LeafQueue 维护的 numContainer  
&ensp;&ensp;&ensp;&ensp; LeafQueue 维护的<b>标签维度</b>的 QueueUsage 中 USED 用途的资源量  
&ensp;&ensp;&ensp;&ensp; LeafQueue 维护的<b>用户标签维度</b>的 QueueUsage 中 USED 用途的资源量  
	3.在节点上申请资源时,按照节点剩余可用资源和资源需求量做除法,若满足需求则分配为 allocatedContainer,不满足则分配 reservedContainer  
	4.若在节点上分配 allocatedContainer,则维护:  
&ensp;&ensp;&ensp;&ensp; Application 方面:newlyAllocatedContainers和 livingContainers 列表,app 已占用资源量    
&ensp;&ensp;&ensp;&ensp; NodeManager 方面:numContainer 数量,NM 可用资源量,NM 已用资源量,NM 启动的 container 列表  
&ensp;&ensp;&ensp;&ensp; QueueMetrics 方面:LeafQueue 及所有 ParentQueue<b>自身QueueMetrics</b>的allocated [containerNum | MB | core]信息;LeafQueue 及其所有 ParentQueue 在<b>用户维度 QueueMetrics</b> 的 allocated [containerNum | MB | cores]信息  
	5.若在节点上分配 reservedContainer,则维护:  
&ensp;&ensp;&ensp;&ensp;Application 方面:reservedContainers 列表,app 已保留资源量  
&ensp;&ensp;&ensp;&ensp;NodeManager 方面:设置 NM 保留的 container 为本次的 reservedContainer  
&ensp;&ensp;&ensp;&ensp;QueueMetrics 方面:LeafQueue 及所有 ParentQueue<b>自身 QueueMetrics</b>的 reserved [containerNum | MB | core]信息;LeafQueue 及其所有 ParentQueue 在<b>用户维度QueueMetrics</b> 的 reserved [containerNum | MB | core ]信息  
	

### 资源释放

以客户端在命令行执行 "yarn application --kill ${APPLICATION_ID}"为例  
RM 中响应请求 方法调用栈为:  
->ClientRMService#forceKillApplication   
&ensp;&ensp;->RMAppImpl$KillAttemptTransition#transition  
&ensp;&ensp;&ensp;&ensp;->RMAppAttemptImpl$BaseFinalTransition#transition  
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;->CapacityScheduler#handle()   

application 结束时,主要做了三件事情,  
1. Container: 释放 APP 持有的<b> living,reserved </b> container  
2. Application(QueueMetrcis): 清理 LeafQueue 及其 ParentQueue 在 <b>APP</b> 层面的统计(e.g. appRunnings);清理 LeafQueue 在<b>用户APP</b> 层面的统计  
3. AM(ResourceUsage): 释放 LeafQueue 及其 ParentQueue 在 <b>AM</b> 层面的资源用量;  释放 LeafQueue 在<b>用户AM</b> 层面的统计(AMUSED)  

由于复现指标异常成功后,发现移动到目标队列后,目标队列增加的资源总量是 spark.executor.memory 的整数倍(executor-mem 11G,driver-memory 2G),所以对上述的第二点和第三点不多分析,主要关注 <b>非 AM 的container</b> 的释放,livingContainers 和 reservedContainer 的释放代码是同一个,仅仅是释放 container 的文字性说明(原文: diagnostics 意为诊断)不同      
[CapactiyScheduler.java]

```
public void handle(SchedulerEvent event) {
....
 case APP_ATTEMPT_REMOVED:
    {
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
      doneApplicationAttempt(appAttemptRemovedEvent.getApplicationAttemptID(),
        appAttemptRemovedEvent.getFinalAttemptState(),
        appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
    }
    break;
    ....
    }
    
private synchronized void doneApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers) {
    
    FiCaSchedulerApp attempt = getApplicationAttempt(applicationAttemptId);
    SchedulerApplication<FiCaSchedulerApp> application =
        applications.get(applicationAttemptId.getApplicationId());
        ....
    //从 app 维护的 livingContainer 列表中,释放掉 allocated acquired running 状态的 container
    for (RMContainer rmContainer : attempt.getLiveContainers()) {
    //在 kill 时keepContainer 为 false;在 failed 时,keepContainer 按场景可为 true
      if (keepContainers
          && rmContainer.getState().equals(RMContainerState.RUNNING)) {
        continue;
      }
      //1.释放 container
      completedContainer(
        rmContainer,
        SchedulerUtils.createAbnormalContainerStatus(
        //COMPLETED_APPLICATION:Container of a completed application
          rmContainer.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.KILL);
    }
	
    //从 app 维护的 reservedContainer 列表中,释放掉 reserved 状态的 container
    for (RMContainer rmContainer : attempt.getReservedContainers()) {
    //1.释放 container
      completedContainer(
        rmContainer,
        SchedulerUtils.createAbnormalContainerStatus(
          rmContainer.getContainerId(), "Application Complete"),
        RMContainerEventType.KILL);
    }
    //2. 清理 pending 的 resourceRequest,并同步 [LeafQueue | ParentQueue] [本身 | 用户]维度的 QueueMetrics appRunnings | appPendings 
    attempt.stop(rmAppAttemptFinalState);
    String queueName = attempt.getQueue().getQueueName();
    CSQueue queue = queues.get(queueName);
    if (!(queue instanceof LeafQueue)) {
      LOG.error("Cannot finish application " + "from non-leaf queue: "
          + queueName);
    } else {
    //3. 同步 [LeafQueue | ParentQueue] [本身 | 用户]维度 QueueUsage 的 AMUSED 指标
      queue.finishApplicationAttempt(attempt, queue.getQueueName());
    }
  }
```
CapacityScheduler#completedContainer主要做了一些校验,避免无效释放  
[CapacityScheduler.java]

```
   protected synchronized void completedContainer(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    if (rmContainer == null) {
      LOG.info("Null container completed...");
      return;
    }
    Container container = rmContainer.getContainer();
    FiCaSchedulerApp application =
        getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
        container.getId().getApplicationAttemptId().getApplicationId();
    if (application == null) {
      LOG.info("Container " + container + " of" + " unknown application "
          + appId + " completed with event " + event);
      return;
    }
     //container 所在的 NM视图
    FiCaSchedulerNode node = getNode(container.getNodeId());
     //代理 container 的 LeafQueue
    LeafQueue queue = (LeafQueue)application.getQueue();
    queue.completedContainer(clusterResource, application, node, 
        rmContainer, containerStatus, event, null, true);
  }
```
先按照 RMContainer 状态机状态判断后续将当前 container 按照 reservedContainer处理或者 allocatedContainer 处理  
然后若释放 container 成功,则同步 用户和标签维度 QueueUsage的 USED 指标,减少 Queue 运行的 container 数量    
最后若当前 Queue 非 rootQueue,则递归同步当前 Queue 的所有 ParentQueue做上述处理  
[LeafQueue.java]

```
public void completedContainer(Resource clusterResource, 
      FiCaSchedulerApp application, FiCaSchedulerNode node, RMContainer rmContainer, 
      ContainerStatus containerStatus, RMContainerEventType event, CSQueue childQueue,
      boolean sortQueues) {
    if (application != null) {
      boolean removed = false;
      synchronized (this) {
        Container container = rmContainer.getContainer();
	// 1. 若 contaienr 状态机为 reserved 状态,在节点上解除 reserve
        if (rmContainer.getState() == RMContainerState.RESERVED) {
          removed = unreserve(application, rmContainer.getReservedPriority(),
              node, rmContainer);
        } else {
        // 2.若 container 状态机不为 reserved 状态,在节点上解除 allocate,并使 NM 释放 container
          removed =
            application.containerCompleted(rmContainer, containerStatus, event);
          node.releaseContainer(container);
        }
        //3.释放 同步 QueueUsage
        if (removed) {
          releaseResource(clusterResource, application,
              container.getResource(), node.getLabels());
          LOG.info("completedContainer" +
              " container=" + container +
              " queue=" + this +
              " cluster=" + clusterResource);
        }
      }
      //4.递归执行 Parent的释放逻辑
      if (removed) {
        getParent().completedContainer(clusterResource, application, node,
          rmContainer, null, event, this, sortQueues);
      }
    }
  }
```
先看第 4 处对 ParentQueue 的处理  
[ParentQueue.java]

```
public void completedContainer(Resource clusterResource,
      FiCaSchedulerApp application, FiCaSchedulerNode node, 
      RMContainer rmContainer, ContainerStatus containerStatus, 
      RMContainerEventType event, CSQueue completedChildQueue,
      boolean sortQueues) {
    if (application != null) {
      synchronized (this) {
      //使用 AbstractCSQueue 中releaseResource方法
        super.releaseResource(clusterResource, rmContainer.getContainer()
            .getResource(), node.getLabels());
	...
        //resort sub-queue
     	....
      }
      // Inform the parent
      if (parent != null) {
 	//递归 ParentQueue#completedContainer 方法
        parent.completedContainer(clusterResource, application, 
            node, rmContainer, null, event, this, sortQueues);
      }    
    }
  }
```
ParentQueue 在 container 释放时,无论是 allocatedContainer 还是 reservedContainer,都更新: 
1.按标签更新 QueueUsage 中 USED 用途的资源量  
2.ParentQueue 中运行 container 的数量 
[AbstractCSQueue.java]

```
protected synchronized void releaseResource(Resource clusterResource,
      Resource resource, Set<String> nodeLabels) {
      //1.QueueUsageByLabel
    if (null == nodeLabels || nodeLabels.isEmpty()) {
      queueUsage.decUsed(resource);
    } else {
      Set<String> anls = (accessibleLabels.contains(RMNodeLabelsManager.ANY))
          ? labelManager.getClusterNodeLabels() : accessibleLabels;
      for (String label : Sets.intersection(anls, nodeLabels)) {
        queueUsage.decUsed(label, resource);
      }
    }
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
        //2.减少当前 Queue 的 container 的数量
    --numContainers;
  }
```
再看第 3 处对 LeafQueue 的处理, 需要注意的是:无论 container 是 allocatedContainer 还是 reversedContainer 在此处都是作为ResourceUsage中 <b>USED</b> 类型的资源来减少的(虽然 ResourceType 枚举中有 RESERVED 类型),且 numContainer 都减少了 1个单位    
[LeafQueue.java]

```
synchronized void releaseResource(Resource clusterResource, 
      FiCaSchedulerApp application, Resource resource, Set<String> nodeLabels) {
    //1.同步当前队列在 标签维度 ResourceUsage 中 USED 类型的资源,并减少 Queue 中 numContainer
    super.releaseResource(clusterResource, resource, nodeLabels);
    //2.同步 app 所属用户在标签维度上的度量信息  
    String userName = application.getUser();
    User user = getUser(userName);
    user.releaseContainer(resource, nodeLabels);
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
  }
```
[AbstractCSQueue.java]  

```
protected synchronized void releaseResource(Resource clusterResource,
      Resource resource, Set<String> nodeLabels) {
     //1.同步 label 层面的 QueueUsage 的资源量;无论 reservedContainer 还是 allocatedContainer 都是使用 #decUsed(resource)
    if (null == nodeLabels || nodeLabels.isEmpty()) {
      queueUsage.decUsed(resource);
    } else {
      Set<String> anls = (accessibleLabels.contains(RMNodeLabelsManager.ANY))
          ? labelManager.getClusterNodeLabels() : accessibleLabels;
      for (String label : Sets.intersection(anls, nodeLabels)) {
        queueUsage.decUsed(label, resource);
      }
    }
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
      //2.减少当前队列启动的 container 数量.无论 reservedContainer 还是 allocatedContainer  
    --numContainers;
  }
```
LeafQueue 中维护了一个 users:Map[String,User]以保存当前各 user 在 USED,PENDING,AMUSED,RESERVED 用途使用资源的度量  
[LeafQueue.java]

```
public void releaseContainer(Resource resource, Set<String> nodeLabels) {
      if (nodeLabels == null || nodeLabels.isEmpty()) {
      //还是 decUsed(resource),无论 reservedContainer 还是 allocatedContainer
        userResourceUsage.decUsed(resource);
      } else {
        for (String label : nodeLabels) {
          userResourceUsage.decUsed(label, resource);
        }
      }
    }
```
在回到LeafQueue#completedContainer方法,按照 RMContainer状态机状态来区分 reservedContainer 还是 allocatedContainer 做处理  
先看对 allocated 的处理逻辑    
[FicaSchedulerApp.java]

```
synchronized public boolean containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    //维护相应的引用列表
    if (null == liveContainers.remove(rmContainer.getContainerId())) {
      return false;
    }
    newlyAllocatedContainers.remove(rmContainer);
    Container container = rmContainer.getContainer();
    ContainerId containerId = container.getId();

    //1.RM 端准备 container 的 结束和清理等任务,待 NM 心跳通过 NodeHeartbeatResponse 交于 NM 做清理
    rmContainer.handle(
        new RMContainerFinishedEvent(
            containerId,
            containerStatus, 
            event)
        );
    ... 
    //2.同步 QueueMetrics
    Resource containerResource = rmContainer.getContainer().getResource();
    queue.getMetrics().releaseResources(getUser(), 1, containerResource);
    //3.减少当前 app 消费的资源
    Resources.subtractFrom(currentConsumption, containerResource);
    ....
    return true;
  }
```
主要看第2步 释放 contaienr 同步 QueueMetrics 的指标和内容,  
1.同步 QueueMetrics 及其 ParentQueue QueueMetrics 的指标: allocate [containerNum | MB | core]  
2.同步 QueueMetrics 及其 ParentQueue QueueMetrics 用户维度的 QueueMetrics 指标: allocate [containerNum | MB | core]  
[QueueMetrics.java] 

```
public void releaseResources(String user, int containers, Resource res) {
    //当前资源队列指标
    allocatedContainers.decr(containers);
    aggregateContainersReleased.incr(containers);
    allocatedMB.decr(res.getMemory() * containers);
    allocatedVCores.decr(res.getVirtualCores() * containers);
    QueueMetrics userMetrics = getUserMetrics(user);
    //资源队列中用户指标
    if (userMetrics != null) {
      userMetrics.releaseResources(user, containers, res);
    }
    //递归 父资源队列
    if (parent != null) {
      parent.releaseResources(user, containers, res);
    }
  }
```
再看释放allocatedContainer 过程中,NM 对该 container 的处理  
1.删掉 launchedContainer 对该 container 的引用  
2.增加该节点的可用资源,减少已经资源,减少启动的 numContainer 指标    
[SchedulerNode.java]

```
public synchronized void releaseContainer(Container container) {
    ..
    //1.删除引用
    if (null != launchedContainers.remove(container.getId())) {
     //2.指标维护
      updateResource(container);
    }
    LOG.info("Released container " + container.getId() + " of capacity "
        + container.getResource() + " on host " + rmNode.getNodeAddress()
        + ", which currently has " + numContainers + " containers, "
        + getUsedResource() + " used and " + getAvailableResource()
        + " available" + ", release resources=" + true);
  }
  private synchronized void updateResource(Container container) {
    addAvailableResource(container.getResource());
    --numContainers;
  }
  private synchronized void addAvailableResource(Resource resource) {
     ...
    Resources.addTo(availableResource, resource);
    Resources.subtractFrom(usedResource, resource);
  }
```
释放 reservedContainer,对 reversedContainer 的处理  
1.application 方面:删除对 reservedContainer 的引用,减少 currentReservation 资源数  
2.nodemanager方面: 设置当前 nm 的reservedContainer 为 null
3.QueueMetrics 方面: 同步[ParentQueue | LeafQueue] [本身 | 用户]维度的指标 reserved [containerNum | MB | core]  
[LeafQueue.java]

```
private boolean unreserve(FiCaSchedulerApp application, Priority priority,
      FiCaSchedulerNode node, RMContainer rmContainer) {
      //1.application: unreserve
    if (application.unreserve(node, priority)) {
     //2. nodemanager: unreserveResource
      node.unreserveResource(application);
     //3.QueueMetrics
      getMetrics().unreserveResource(application.getUser(),
          rmContainer.getContainer().getResource());
      return true;
    }
    return false;
  }
```
application 和 nodemanager 层面的逻辑比较简单,且看 QueueMetrics 方面的同步  
[QueueMetrics.java]

```
public void unreserveResource(String user, Resource res) {
  //当前资源队列 QueueMetrics的 reserved [container | MB | core] 指标
    reservedContainers.decr();
    reservedMB.decr(res.getMemory());
    reservedVCores.decr(res.getVirtualCores());
    //当前资源队列 在用户维度的 reserved [container | MB | core] 指标
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.unreserveResource(user, res);
    }
    //父资源队列在 其本身和用户维度的 reserved [container | MB | core] 指标
    if (parent != null) {
      parent.unreserveResource(user, res);
    }
  }
```

>
综上:   
CS 调度器在为 APP 释放container 时,会同步 [Parent | Leaf]Queue/APP/NM 三个位置维护的信息  
1. 释放时,无论是释放 allocated 或 reserved 类型的 container,其 <b>ParentQueue</b> 都会维护:
&ensp;&ensp;&ensp;&ensp;ParentQueue 维护的 numContainer  
&ensp;&ensp;&ensp;&ensp;ParentQueue 维护的<b>标签维度</b>的QueueUsage 中 USED 用途的资源量  
2. 释放时,无论是释放 allocated 或 reserved 类型的 container,其 <b>LeafQueue</b> 都会维护:   
&ensp;&ensp;&ensp;&ensp;LeafQueue 维护的 numContainer  
&ensp;&ensp;&ensp;&ensp;LeafQueue 维护的<b>标签维度</b>的 QueueUsage 中 USED 用途的资源量  
&ensp;&ensp;&ensp;&ensp;LeafQueue 维护的<b>用户标签维度</b>的 QueueUsage 中 USED 用途的资源量  
3.释放 container 时,按照该 container 的状态机状态做细分,分为 reservedContainer 和 allocatedContainer 两类做处理  
4.若在节点上释放 allocatedContainer,则维护:  
&ensp;&ensp;&ensp;&ensp;Application 方面:newlyAllocatedContainers 和 reservedContainers 列表,app已占用资源量    
&ensp;&ensp;&ensp;&ensp;NodeManager方面:numContainer数量,NM 可用资源量,NM 已用资源量,NM 启动的 container 列表    
&ensp;&ensp;&ensp;&ensp;QueueMetrics 方面:LeafQueue 和所有 ParentQueue <b>自身 QueueMetrics</b> 的 reserved [containerNum | MB | core]信息;LeafQueue 和所有 ParentQueue 在<b>用户维度 QueueMetrics </b> 的  reserved [containerNum | MB | core]信息  
5.若在节点上释放 reservedContainer,则维护:  
&ensp;&ensp;&ensp;&ensp;Application 方面:reservedContainers 列表,app 已保留资源量  
&ensp;&ensp;&ensp;&ensp; NodeManager 方面:设置 NM 保留的 container 为 null    
&ensp;&ensp;&ensp;&ensp; QueueMetrics 方面:LeafQueue 及所有 ParentQueue<b>自身 QueueMetrics</b>的 reserved [containerNum | MB | core]信息;LeafQueue 及其所有 ParentQueue 在<b>用户维度QueueMetrics</b> 的 reserved [containerNum | MB | core ]信息  


Container 释放过程和申请过程 对于 Queue/Application/NM三个位置维护的 Queue/QueueUsageByLabel/QueueUsageByUser/QueueMetrics信息,恰好是一一对应的  
 
对于 YarnUI 指标异常问题,比较关注的 Queue.numContainer 和 QueueUsageByLabel.resArr[ResourceType.USED]两个指标,一次 allocated/reversed/Container 的申请对应着 numContainer++和 ResourceUsage.incUsed;一次 container 的释放对应着 numContainer--和 ResourceUsage.decUsed   

### movetoqueue 时资源转移过程


使用命令 "yarn application -movetoqueue ${APPID} -queue ${TO}"
可以将 ${APPID}的 APP 移动到目标资源队列 ${TO}  
ClientRMService 相应请求的调用栈是:  
->ClientRMService#moveApplicationAcrossQueues  
&ensp;&ensp;->RMAppImpl$RMAppMoveTransition#transition  
&ensp;&ensp;&ensp;&ensp;->CapacityScheduler#moveApplication  

application 从源资源队列移动到目标资源队列分为几步:  
1. 向目标队列提交application 并做校验,包括 access 权限、目标队列的 app 数量超限、目标队列中用户提交 app 数量超限,若提交成功则增加所有 ParentQueue 的 numApplication 数量(但是没有增加自身的 numApplication 数量)    
2. 转移源队列中所有非 reserved 的 container 到目标队列,源队列(LeafQueue)释放 container,减少<b>标签维度和用户维度</b>的 ResourceUsage USED 类型资源量,并减少源队列的所有父队列(ParentQueue) <b>标签维度</b>的 ResourceUsage USED 类型资源量;增加目标队列(LeafQueue) <b>标签维度和用户维度</b>的 ResourceUsage USED 类型资源量,并增加目标队列的所有父队列(ParentQueue) <b>标签维度</b>的 ResourceUsage USED 类型资源量.指标数量维护在Queue/ResourceUsage/QueueMetrics 中,此处没有处理 livingContainers 的 QueueMetrics ,在第 5 步中统一处理 QueueMetrics    
3. 源队列中移除 appAttemptd,并同步 QueueUsageByUser 和 QueueUsageByLabel 中 AMUSED类型的资源量  
4. 在源队列的所有 ParentQueue 中移除此 Application    
5. 同步源队列和目标队列中所有非 AM container 移动导致的 QueueMetrics 变化  
6. 同步用户维度的 appAttempt 统计信息   
[CapacityScheduler.java]  

```
 public synchronized String moveApplication(ApplicationId appId,
      String targetQueueName) throws YarnException {
    FiCaSchedulerApp app =
        getApplicationAttempt(ApplicationAttemptId.newInstance(appId, 0));
    String sourceQueueName = app.getQueue().getQueueName();
    LeafQueue source = getAndCheckLeafQueue(sourceQueueName);
    String destQueueName = handleMoveToPlanQueue(targetQueueName);
    LeafQueue dest = getAndCheckLeafQueue(destQueueName);
    String user = app.getUser();
    try {
      //1. 转移 Application
      dest.submitApplication(appId, user, destQueueName);
    } catch (AccessControlException e) {
      throw new YarnException(e);
    }
    //2.转移 livingContainer
    for (RMContainer rmContainer : app.getLiveContainers()) {
      source.detachContainer(clusterResource, app, rmContainer);
      // attach the Container to another queue
      dest.attachContainer(clusterResource, app, rmContainer);
    }
    //3.源资源队列移除attempt,并同步QueueUsage
    source.finishApplicationAttempt(app, sourceQueueName);
    //4.源队列的父队列移除 application
    source.getParent().finishApplication(appId, app.getUser());
   //5.同步源队列和目标队列的 QueueMetrics
    app.move(dest);
    //6.提交attemp
    dest.submitApplicationAttempt(app, user);
    applications.get(appId).setQueue(dest);
    LOG.info("App: " + app.getApplicationId() + " successfully moved from "
        + sourceQueueName + " to: " + destQueueName);
    return targetQueueName;
  }
```
第 2 步中,遍历 app 的 livingContainer,把每个 container 从源队列及父队列释放,减少源队列及父队列的指标  
先看源队列 LeafQueue 的释放逻辑  
[LeafQueue.java]

```
public void detachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      //AbstractCSQueue#releaseResource
      releaseResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), node.getLabels());
      LOG.info("movedContainer" + " container=" + rmContainer.getContainer()
          + " resource=" + rmContainer.getContainer().getResource()
          + " queueMoveOut=" + this + " usedCapacity=" + getUsedCapacity()
          + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + " used="
          + queueUsage.getUsed() + " cluster=" + clusterResource);
      //ParentQueue#detachContainer
      getParent().detachContainer(clusterResource, application, rmContainer);
    }
  }
  
  synchronized void releaseResource(Resource clusterResource, 
      FiCaSchedulerApp application, Resource resource, Set<String> nodeLabels) {
      //AbstractCSQueue#releaseResource
     //1.减少 标签维度的 ResourceUsage USED 用途的资源;并减少当前 Queue 的 numContainer 数量
    super.releaseResource(clusterResource, resource, nodeLabels);
    //2.减少 提交用户在 标签维度的 ResourceUsage USED 用途的资源
    String userName = application.getUser();
    User user = getUser(userName);
    user.releaseContainer(resource, nodeLabels);
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
    ...
  }
  
  //AbstractCSQueue#releaseResource
  //ResourceUsageByLabel USED 用途的资源量
  protected synchronized void releaseResource(Resource clusterResource,
      Resource resource, Set<String> nodeLabels) {
    if (null == nodeLabels || nodeLabels.isEmpty()) {
      queueUsage.decUsed(resource);
    } else {
      Set<String> anls = (accessibleLabels.contains(RMNodeLabelsManager.ANY))
          ? labelManager.getClusterNodeLabels() : accessibleLabels;
      for (String label : Sets.intersection(anls, nodeLabels)) {
        queueUsage.decUsed(label, resource);
      }
    }
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
        //Queue 的运行 container 数量
    --numContainers;
  }
  //第 2 处,ResourceUsageByUser USED 用途的资源量
public void releaseContainer(Resource resource, Set<String> nodeLabels) {
      if (nodeLabels == null || nodeLabels.isEmpty()) {
        userResourceUsage.decUsed(resource);
      } else {
        for (String label : nodeLabels) {
          userResourceUsage.decUsed(label, resource);
        }
      }
```
ParentQueue 释放 container 时,减少了用户维度的 ResourceUsage 在 USED 用途上资源量  
[ParentQueue.java]

```
public void detachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      //AbstractCSQueue#releaseResource
      super.releaseResource(clusterResource,
          rmContainer.getContainer().getResource(),
          node.getLabels());
      LOG.info("movedContainer" + " queueMoveOut=" + getQueueName()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + queueUsage.getUsed() + " cluster="
          + clusterResource);
      // Inform the parent
      if (parent != null) {
        //递归父队列
        parent.detachContainer(clusterResource, application, rmContainer);
      }
    }
  }
  //AbstractCSQueue#releaseResource
  protected synchronized void releaseResource(Resource clusterResource,
      Resource resource, Set<String> nodeLabels) {
    // Update usedResources by labels
    if (null == nodeLabels || nodeLabels.isEmpty()) {
      queueUsage.decUsed(resource);
    } else {
      Set<String> anls = (accessibleLabels.contains(RMNodeLabelsManager.ANY))
          ? labelManager.getClusterNodeLabels() : accessibleLabels;
      for (String label : Sets.intersection(anls, nodeLabels)) {
        queueUsage.decUsed(label, resource);
      }
    }
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
    --numContainers;
  }
```
将 container 从源队列转移到目标队列时,会增加目标队列及其父队列的指标   
先看目标队列(LeafQueue)增加的指标   
[LeafQueue.java]

```
public void attachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
          //1.增加 LeafQueue 标签维度的 ResourceUsage 和用户维度的 ResourceUsage, USED 类型的资源量;并增加numContainer
      allocateResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), node.getLabels());
      LOG.info("movedContainer" + " container=" + rmContainer.getContainer()
              //add by jiulong.zhu@20190903
              +" containerState="+rmContainer.getState()
          + " resource=" + rmContainer.getContainer().getResource()
          + " queueMoveIn=" + this + " usedCapacity=" + getUsedCapacity()
          + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + " used="
          + queueUsage.getUsed() + " cluster=" + clusterResource);
      //2.增加 ParentQueue 标签维度的 ResourceUsage USED 类型的资源量;并增加 ParentQueue 的 numContainer 数量
      getParent().attachContainer(clusterResource, application, rmContainer);
    }
  }
  
  synchronized void allocateResource(Resource clusterResource,
      SchedulerApplicationAttempt application, Resource resource,
      Set<String> nodeLabels) {
      //AbstractCSQueue#allocateResource 增加标签维度的 ResourceUsage USED 用途的用量;增加 Queue 的numContainers  
    super.allocateResource(clusterResource, resource, nodeLabels);
    String userName = application.getUser();
    //增加当前用户 标签维度的 ResourceUsage USED 用途的用量  
    User user = getUser(userName);
    user.assignContainer(resource, nodeLabels);
    
    Resources.subtractFrom(application.getHeadroom(), resource);
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
    if (LOG.isDebugEnabled()) {
      LOG.info(getQueueName() + 
          " user=" + userName + 
          " used=" + queueUsage.getUsed() + " numContainers=" + numContainers +
          " headroom = " + application.getHeadroom() +
          " user-resources=" + user.getUsed()
          );
    }
  }
    
    //AbstractCSQueue#allocateResource
   synchronized void allocateResource(Resource clusterResource, 
      Resource resource, Set<String> nodeLabels) {
    if (nodeLabels == null || nodeLabels.isEmpty()) {
      queueUsage.incUsed(resource);
    } else {
      Set<String> anls = (accessibleLabels.contains(RMNodeLabelsManager.ANY))
          ? labelManager.getClusterNodeLabels() : accessibleLabels;
      for (String label : Sets.intersection(anls, nodeLabels)) {
        queueUsage.incUsed(label, resource);
      }
    }
    ++numContainers;
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
  }
```
再看将 container 移动到目标队列时,目标队列的 ParentQueue 增加的逻辑  
[ParentQueue.java]

```
public void attachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
          //1.增加 ParentQueue 在标签维度的 ResourceUsage USED 用量 
          //AbstractCSQueue#allocateResource
      super.allocateResource(clusterResource, rmContainer.getContainer()
          .getResource(), node.getLabels());
      LOG.info("movedContainer" + " queueMoveIn=" + getQueueName()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + queueUsage.getUsed() + " cluster="
          + clusterResource);
      //2.递归增加 ParentQueue 的所有 ParentQueue 的 ResourceUsage USED 用量  
      if (parent != null) {
        parent.attachContainer(clusterResource, application, rmContainer);
      }
    }
  }
  
  //AbstractCSQueue#allocateResource
  synchronized void allocateResource(Resource clusterResource, 
      Resource resource, Set<String> nodeLabels) {
    if (nodeLabels == null || nodeLabels.isEmpty()) {
      queueUsage.incUsed(resource);
    } else {
      Set<String> anls = (accessibleLabels.contains(RMNodeLabelsManager.ANY))
          ? labelManager.getClusterNodeLabels() : accessibleLabels;
      for (String label : Sets.intersection(anls, nodeLabels)) {
        queueUsage.incUsed(label, resource);
      }
    }
    ++numContainers;
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
  }
```
第 3 步:删除源队列中该 app 的 currentApplicationAttempt,并同步 LeafQueue 中用户提交的 app 数量变化     
[LeafQueue.java]

```
public void finishApplicationAttempt(FiCaSchedulerApp application, String queue) {
    synchronized (this) {
      removeApplicationAttempt(application, getUser(application.getUser()));
    }
    //空实现
    getParent().finishApplicationAttempt(application, queue);
  }
  
  public synchronized void removeApplicationAttempt(
      FiCaSchedulerApp application, User user) {
    boolean wasActive = activeApplications.remove(application);
    if (!wasActive) {
      pendingApplications.remove(application);
    } else {
      //正在运行的 app_attempt,则减少 LeafQueue 的 [ResourceUsage | ResourceUsageByUser] 中AMUSED 占用的资源量
      queueUsage.decAMUsed(application.getAMResource());
      user.getResourceUsage().decAMUsed(application.getAMResource());
    }
    applicationAttemptMap.remove(application.getApplicationAttemptId());
     // 在 ResourceUsageByUser 同步 numApplication 指标
    user.finishApplication(wasActive);
    if (user.getTotalApplications() == 0) {
      users.remove(application.getUser());
    }
    // Check if we can activate more applications
    activateApplications();
    LOG.info("Application removed -" +
        " appId: " + application.getApplicationId() + 
        " user: " + application.getUser() + 
        " queue: " + getQueueName() +
        " #user-pending-applications: " + user.getPendingApplications() +
        " #user-active-applications: " + user.getActiveApplications() +
        " #queue-pending-applications: " + getNumPendingApplications() +
        " #queue-active-applications: " + getNumActiveApplications()
        );
  }
```
第 4 步:对于源队列的所有 ParentQueue,移除 app 的 currentApplicationAttempt,并减少资源队列的运行 numApplication 值    
[ParentQueue.java]

```
public void finishApplication(ApplicationId application, String user) {
    synchronized (this) {
      removeApplication(application, user);
    }
    //递归 父队列
    if (parent != null) {
      parent.finishApplication(application, user);
    }
  }
  
  private synchronized void removeApplication(ApplicationId applicationId, 
      String user) {
    --numApplications;
    LOG.info("Application removed -" +
        " appId: " + applicationId + 
        " user: " + user + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());
  }
```
第 5 步: 将 app 持有的所有 allocatedContainer(包括 AM container)和 reservedContainer,移动到目标队列,并修改 QueueMetrics  
[SchedulerApplicationAttempt.java]

```
public synchronized void move(Queue newQueue) {
    QueueMetrics oldMetrics = queue.getMetrics();
    QueueMetrics newMetrics = newQueue.getMetrics();
    String user = getUser();
    //修改 livingContainer 的 QueueMetrics
    for (RMContainer liveContainer : liveContainers.values()) {
      Resource resource = liveContainer.getContainer().getResource();
      oldMetrics.releaseResources(user, 1, resource);
      newMetrics.allocateResources(user, 1, resource, false);
    }
    //修改 reservedContainer的 QueueMetrics
    for (Map<NodeId, RMContainer> map : reservedContainers.values()) {
      for (RMContainer reservedContainer : map.values()) {
        Resource resource = reservedContainer.getReservedResource();
        oldMetrics.unreserveResource(user, resource);
        newMetrics.reserveResource(user, resource);
      }
    }
    //移动 pending ResourceRequest,同步信息:QueueMetrics.appsRunning,Queue下的用户列表    
    appSchedulingInfo.move(newQueue);
    this.queue = newQueue;
  }
```
修改 livingContainers 和 reservedContainers 的所有逻辑都在 QueueMetrics中,其中  
1.releaseResources,释放 livingContainer: [ParentQueue | LeafQueue] [本身 | 用户维度] 的 allocated [Containers | MB | Core]指标  
2.allocateResource,申请 livingContainer: [ParentQueue | LeafQueue] [本身 | 用户维度] 的 allocated [Containers | MB | Core]指标  
3.unreserveResource,释放 reservedContainer:[ParentQueue | LeafQueue] [本身 | 用户维度] 的 reserved [Containers| MB | Core]指标  
4.reserveResource,申请 reservedContainer:[ParentQueue | LeafQueue] [本身 | 用户维度] 的 reserved [Container | MB | Core]指标  
[QueueMetrics.java]

```
public void releaseResources(String user, int containers, Resource res) {
    allocatedContainers.decr(containers);
    aggregateContainersReleased.incr(containers);
    allocatedMB.decr(res.getMemory() * containers);
    allocatedVCores.decr(res.getVirtualCores() * containers);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.releaseResources(user, containers, res);
    }
    if (parent != null) {
      parent.releaseResources(user, containers, res);
    }
  }
  
 public void allocateResources(String user, int containers, Resource res,
      boolean decrPending) {
    allocatedContainers.incr(containers);
    aggregateContainersAllocated.incr(containers);
    allocatedMB.incr(res.getMemory() * containers);
    allocatedVCores.incr(res.getVirtualCores() * containers);
    if (decrPending) {
      _decrPendingResources(containers, res);
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.allocateResources(user, containers, res, decrPending);
    }
    if (parent != null) {
      parent.allocateResources(user, containers, res, decrPending);
    }
  }
  
  public void unreserveResource(String user, Resource res) {
    reservedContainers.decr();
    reservedMB.decr(res.getMemory());
    reservedVCores.decr(res.getVirtualCores());
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.unreserveResource(user, res);
    }
    if (parent != null) {
      parent.unreserveResource(user, res);
    }
  }
  
  public void reserveResource(String user, Resource res) {
    reservedContainers.incr();
    reservedMB.incr(res.getMemory());
    reservedVCores.incr(res.getVirtualCores());
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.reserveResource(user, res);
    }
    if (parent != null) {
      parent.reserveResource(user, res);
    }
  }
  
```
第 6 步:同步目标队列 application 和 applicationAttempt信息    
[LeafQueue.java]

```
public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName) {
    synchronized (this) {
      User user = getUser(userName);
      //增加 Queue 和 User 维度 application / applicationAttempt 的值
      addApplicationAttempt(application, user);
    }
    // We don't want to update metrics for move app
    if (application.isPending()) {
     //增加 Queue 和 User 维度 QueueMetrics 中 appsPending 的值
      metrics.submitAppAttempt(userName);
    }
    //空实现
    getParent().submitApplicationAttempt(application, userName);
  }
```

>综上:  
把 application 从源资源队列到目标队列的过程中,在 Queue/ResourceUsageByLabel/ResourceUsageByUser/QueueMetrics 中的变动    
1.对于 livingContainers,  
&ensp;&ensp;&ensp;&ensp;Queue 方面:减少源队列及其父队列的 numContainer 值;增加目标队列及其父队列的 numContainer  
&ensp;&ensp;&ensp;&ensp;ResourceUsage 方面:减少源队列及其父队列的 ResourceUsageByLabel USED 类型的资源量,增加目标队列及其父队列的 ResourceUsageByLabel USED 类型的资源量; 减少源队列(LeafQueue)的 ResourceUsageByUser USED 类型的资源量,增加目标队列(LeafQueue) 的 ResourceUsageByUser USED 类型的资源量;  
&ensp;&ensp;&ensp;&ensp;QueueMetrics 方面:减少源队列及其父队列的 allocated[Container | MB | Core]指标,增加目标队列及其父队列的 allocated [Container | MB | Core]  
2.对于 reservedContainers,  
&ensp;&ensp;&ensp;&ensp;ResourceUsage:减少源队列[LeafQueue | ParentQueue] [本身 | 用户维度] 的reserved [Container | MB | Core]指标,增加目标队列[LeafQueue | ParentQueue] [本身 | 用户维度] 的 reserved [Container | MB | Core]指标  


### 结论  

结合 "正常的资源分配和释放过程"和"movetoqueue 过程中的资源转移过程 ",可以还原一个被转移队列的 application 的资源变动过程  
application 中非 reservedContianer(状态机:allocated,acquired,running) 的资源变动过程如下表  

| allocatedContainer  | 分配 | movetoqueue | 释放 | 
|:------------- |:---------------:| :---------------:|:---------------:|
|numContainer|源队列值增加|源队列减少,目标队列增加|目标队列值减少|
|ResourceUsageByLabel(USED)|源队列值增加|源队列减少,目标队列增加|目标队列值减少|
|QueueMetrics|源队列值增加|源队列减少,目标队列增加|目标队列值减少|

application 中 reservedContianer(状态机:reversed) 的资源变动过程如下表   

| reversedContainer  | 分配 | movetoqueue| 释放 | 
|:------------- |:---------------:| :---------------:|:---------------:|
|numContainer|源队列值增加|<font color="#660000">源队列不变,目标队列不变</font>|目标队列值减少|  
|ResourceUsageByLabel(USED)|源队列值增加|<font color="#660000">源队列不变,目标队列不变</font>|目标队列值减少|  
|QueueMetrics|源队列值增加|源队列减少,目标队列增加|目标队列值减少|  

>
从上述两表中可知:  
1.在没有 movetoqueue 操作的情况下, 源队列和目标队列一致,无论 allocatedContainer 或 reservedContainer 的分配和释放都是守恒的,一次指标的增加对应着一次指标的减少  
2.在源队列资源比较充裕,且有 movetoqueue 操作的情况下,application 没有 reservedContainer 的情况下(上述表一),在分配时源队列指标增加,movetoqueue 时,源队列减少指标且目标队列增加指标,在释放时减少目标队列(app 当前归属队列)的指标.指标的增减也是平衡的,这也就是在资源充裕的队列上无法复现该问题的原因  
3.在源队列资源比较紧张,且有 movetoqueue 操作的情况下,application 有 reservedContainer 和 allocatedContainer(上述表一和表二),对于 allocatedContainer 来说,资源在分配->movetoque->释放的过程中是平衡的(表一);但是对于 reversedContainer 来说,在源队列中分配到资源时,增加了源队列的 numContainer 和 ResourceUsageByLabel 中 USED 用途的资源量,movetoqueue 时没有相应的操作,仅仅是修改了 QueueMetrcis 内的指标,在释放时 container 资源被加入到目标队列的指标中(上述表二).<b>源队列分配出去的资源没有回收,目标队列得到了不是自身分配出去的资源</b>.这样导致了即使源队列和目标队列中完全无任务时,源队列的 numContainer 和 ResourceUsageByLabel 值为正值,目标队列的 numContainer 和 ResourceUsage 值为负值,Yarn UI ->scheduler->queue 内的 Num Containers/Used Capacity/Absolute Used Capacity/Used Resources 正是基于 队列的 numContainer 和 ResourceUsageByLabel(USED) 展示和计算的,也就是指标异常的问题     

指标异常(无任务时 为负)
![](/img/pictures/negative/negative_e7a766275896.png)

指标异常(无任务时 为正)
![](/img/pictures/negative/negative_aead-04d8e67b1357.png)

### 危害

1.对于源队列来说,类似于资源泄露,自身资源未能回收,释放到了目标队列.这样对于后续提交到源队列的任务来说,不能分配到资源运行(USED 泄露趋于 100%),但实际上源队列没有任务在运行    
2.对于目标队列来说,多出了一些资源,导致自身一些指标为负,可能会导致一些未知的问题.以下纯属个人猜测: 例如 同步问题,Queue 有很多的剩余资源,但是 NodeManager 上没有 available 资源;指标负值可能对内部的运算产生未预期的问题  

### 解决方法

针对 reservedContainer 在 movetoqueue 操作过程中,资源释放申请不守恒的漏洞,如下表  

| reversedContainer  | 分配 | movetoqueue| 释放 | 
|:------------- |:---------------:| :---------------:|:---------------:|
|numContainer|源队列值增加|<font color="#660000">源队列不变,目标队列不变</font>|目标队列值减少|  
|ResourceUsageByLabel(USED)|源队列值增加|<font color="#660000">源队列不变,目标队列不变</font>|目标队列值减少|  
|QueueMetrics|源队列值增加|源队列减少,目标队列增加|目标队列值减少|  

>
问题:    
&ensp;&ensp;&ensp;&ensp;YarnUI 中指标 Num Container、Used Capacity、Absolute Used Capacity、Used Resource 异常和 Queue成员变量numContainer及 QueueUsageByLabel 中 USED 用途的资源量有关    
目标:  
&ensp;&ensp;&ensp;&ensp;解决 numContainer 和 ResourceUsageByLabel(USED)在 movetoqueue 时,没有"减少源队列的指标"且没有"增加目标队列指标"的问题  
途径:  
&ensp;&ensp;&ensp;&ensp;1.需要在 movetoqueue 时,每个 reservedContainer 都应触发 "源队列减少指标"和"目标队列增加指标"的行为 
&ensp;&ensp;&ensp;&ensp;源码中有封装好的代码且上文多次提到,即 LeafQueue#releaseResource方法和 LeafQueue.allocateResource方法,但是其中有额外的 对 headroom 的同步逻辑.一方面,在"正常的分配和释放"过程中,无论申请和分配的 container 是 allocated 或是 reversed 都会执行 LeafQueue#releaseResource 和 LeafQueue.allocateResource 修改 numContainer 和 ResourceUsageByLabel 的同时修改 headroom值;另一方面 move container中对 headroom 增加和减少 同一个Resource 是幂等的.所以 movetoqueue 操作对于每一个 container 的移动都可以选择触发这两个方法来增加对 numContainer 和 ResourceUsageByLabel 的操作.LeafQueue#detachContainer()和 LeafQueueattachContainer()  中有递归本队列及当前队列此逻辑的过程.直接用即可       
修改方案:  
&ensp;&ensp;&ensp;&ensp;CapacitySchedule#moveApplication(ApplicationId appId,String targetQueueName) 方法修改如下  

[CapacityScheduler.java]

```
@Override
  public synchronized String moveApplication(ApplicationId appId,
      String targetQueueName) throws YarnException {
    FiCaSchedulerApp app =
        getApplicationAttempt(ApplicationAttemptId.newInstance(appId, 0));
    String sourceQueueName = app.getQueue().getQueueName();
    LeafQueue source = getAndCheckLeafQueue(sourceQueueName);
    String destQueueName = handleMoveToPlanQueue(targetQueueName);
    LeafQueue dest = getAndCheckLeafQueue(destQueueName);
    // Validation check - ACLs, submission limits for user & queue
    String user = app.getUser();
    try {
      dest.submitApplication(appId, user, destQueueName);
    } catch (AccessControlException e) {
      throw new YarnException(e);
    }
    // Move all live containers
    for (RMContainer rmContainer : app.getLiveContainers()) {
      source.detachContainer(clusterResource, app, rmContainer);
      // attach the Container to another queue
      dest.attachContainer(clusterResource, app, rmContainer);
    }
    //description:解决 reservedContainer 在 movetoqueue 操作过程中,Queue.numContainer 指标和 ResourceUsageByLabel.USED 资源量
    //释放申请不守恒,导致 Yarn UI "Num Container","Used Capacity","Absolute Used Capacity","Used Resource"
    //指标在队列无任务运行时不为 0 的问题
    //modify by jiulong.zhu@20190903
    //++add start
    for (RMContainer rmContainer : app.getReservedContainers()) {
      source.detachContainer(clusterResource, app, rmContainer);
      dest.attachContainer(clusterResource, app, rmContainer);
    }
    //++add end

    // Detach the application..
    source.finishApplicationAttempt(app, sourceQueueName);
    source.getParent().finishApplication(appId, app.getUser());
    // Finish app & update metrics
    app.move(dest);
    // Submit to a new queue
    dest.submitApplicationAttempt(app, user);
    applications.get(appId).setQueue(dest);
    LOG.info("App: " + app.getApplicationId() + " successfully moved from "
        + sourceQueueName + " to: " + destQueueName);
    return targetQueueName;
  }
```

## 冒烟测试用例

[TestCapacityScheduler.java]

```
  @Test
  public void testReservedContainerLeakWhenMoveApplication() throws Exception {
    CapacitySchedulerConfiguration csConf
            = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    csConf.setCapacity("root.a",50);
    csConf.setMaximumCapacity("root.a",100);
    csConf.setUserLimitFactor("root.a",100);
    csConf.setCapacity("root.b",50);
    csConf.setMaximumCapacity("root.b",100);
    csConf.setUserLimitFactor("root.b",100);

    YarnConfiguration conf=new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
            ResourceScheduler.class);
    RMNodeLabelsManager mgr=new NullRMNodeLabelsManager();
    mgr.init(conf);
    MockRM rm1 = new MockRM(csConf);
    CapacityScheduler scheduler=(CapacityScheduler) rm1.getResourceScheduler();
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("127.0.0.1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("127.0.0.2:1234", 8 * GB);
    /**
     * simulation
     * app1: (1 AM,1 running container)
     * app2: (1 AM,1 reserved container)
     */
    // launch an app to queue, AM container should be launched in nm1
    RMApp app1 = rm1.submitApp(1 * GB, "app_1", "user_1", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm1
    RMApp app2 = rm1.submitApp(1 * GB, "app_2", "user_1", null, "a");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    am1.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());
    //this containerRequest should be reserved
    am2.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());

    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    // Do node heartbeats 2 times
    // First time will allocate container for app1, second time will reserve
    // container for app2
    scheduler.handle(new NodeUpdateSchedulerEvent(rmNode1));
    scheduler.handle(new NodeUpdateSchedulerEvent(rmNode1));

    FiCaSchedulerApp schedulerApp1 =
            scheduler.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
            scheduler.getApplicationAttempt(am2.getApplicationAttemptId());
    // APP1:  1 AM, 1 allocatedContainer
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    // APP2:  1 AM,1 reservedContainer
    Assert.assertEquals(1,schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(1,schedulerApp2.getReservedContainers().size());
    /**
     * before,move app2 which has one reservedContainer
     */
    LeafQueue srcQueue = (LeafQueue) scheduler.getQueue("a");
    LeafQueue desQueue = (LeafQueue) scheduler.getQueue("b");
    Assert.assertEquals(4,srcQueue.getNumContainers());
    Assert.assertEquals(10*GB,srcQueue.getUsedResources().getMemorySize());// AM: 2*1GB   container: 4GB running,4GB reserved
    Assert.assertEquals(0,desQueue.getNumContainers());
    Assert.assertEquals(0,desQueue.getUsedResources().getMemorySize());
    //app1 ResourceUsage (0 reserved)
    Assert.assertEquals(5*GB,schedulerApp1.getAppAttemptResourceUsage().getAllUsed().getMemorySize());
    Assert.assertEquals(0,schedulerApp1.getCurrentReservation().getMemorySize());
    //app2  ResourceUsage (4GB reserved)
    Assert.assertEquals(1*GB,schedulerApp2.getAppAttemptResourceUsage().getAllUsed().getMemorySize());
    Assert.assertEquals(4*GB,schedulerApp2.getCurrentReservation().getMemorySize());
    /**
     * move app2 which has one reservedContainer
     */
    scheduler.moveApplication(app2.getApplicationId(),"b");
    // finish.keep the order,if killing app1 first,the reservedContainer of app2 will be allocated
    rm1.killApp(app2.getApplicationId());
    rm1.killApp(app1.getApplicationId());
    /**
     * after,moved app2 which has one reservedContainer
     */
    {
      // after fixed
      Assert.assertEquals(0, srcQueue.getNumContainers());
      Assert.assertEquals(0, desQueue.getNumContainers());
      Assert.assertEquals(0, srcQueue.getUsedResources().getMemorySize());
      Assert.assertEquals(0, desQueue.getUsedResources().getMemorySize());
    }
    /*{
      // before fixed
      // <b> the reserved container borrowed from srcQueue and returned to desQueue,
      // but the numContainer and UsedResource did not sync when moving app to another queue </b>
      Assert.assertEquals(+1,srcQueue.getNumContainers());    //true
      Assert.assertEquals(-1,desQueue.getNumContainers());    //true
      Assert.assertEquals(+4*GB, srcQueue.getUsedResources().getMemorySize());    //true
      Assert.assertEquals(-4*GB, desQueue.getUsedResources().getMemorySize());    //true
    }*/
    rm1.close();
  }
```

## 线上测试用例


