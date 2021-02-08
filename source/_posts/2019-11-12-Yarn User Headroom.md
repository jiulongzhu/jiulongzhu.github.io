---
layout:     post
title:      Yarn User Headroom
date:       2019-11-12
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - Yarn 2.7.3
    - 源码解析
---

ResourceManager 尝试在 NM 上分配容器的过程中，主要考虑的限制条件如下:   
a)	队列具有该节点的 access 权限,节点不在 APP 设置的黑名单内  
b)	节点上无保留且节点剩余空间大于最小容器大小(1G)  
c)	最高配额限制:如果分配该容器,叶子队列及其所有父队列均不能超过各自的最高配额。  
d)	用户配额限制(headroom):瞬时量,该容器大小不能超过 headroom 当前值。    
e)	AM 限制:不超过队列 AM 限制(AM资源和AM数量限制)  
主要限制在 c)和 d)。c)是在在队列层面对队列可使用资源量做的限制; d) user headroom 是在用户层面对用户可使用资源量做的限制。只有在不违背上述条件时，容器才会在当前 NM 上分配或保留。

<!-- more -->

>
user headroom 的计算过程主要有三步,  
第一步: 计算队列的配置容量 queueCapacity，即 max{capacity, required}        
第二步: 计算队列的当前容量 currentCapacity，即分段函数 capacity 和 used + required   
第三步: 计算用户限制 limit，即 min{queueCapacity * userLimitFactor, max{currentCapacty/activeUser, currentCapacity * userLimit/100}}  

主要和两个配置有关:  
1.  yarn.scheduler.capacity.<queue-path>.minimum-user-limit-percent(程序内对应 userLimit)  
　表示队列内多用户均分资源的方式，默认为 100。此值可以在一个极小值与极大值之间动态变化,该配置决定极小值,极大值由队列内活跃用户数决定。e.g. 将此值配置为 30 时, 当队列内仅有1个用户时,该用户最多使用队列100%资源;2个用户,每个用户最多使用50%资源;3个用户,每个用户最多可以使用 33.3%资源;4 个及更多时 每个用户最多可以使用 30%资源。即 [0,max{0.3,1/activeUser}]，体现在源码中   
　

```
　Resources.max(
                    resourceCalculator, clusterResource,
                    //当前容量/活跃用户数
                    Resources.divideAndCeil(
                        resourceCalculator, currentCapacity, activeUsers),
                    //当前容量 * 用户限制/100
                    Resources.divideAndCeil(
                        resourceCalculator,
                        Resources.multiplyAndRoundDown(
                            currentCapacity, userLimit),
                        100)
                    );
```
2.  yarn.scheduler.capacity.<queue-path>.user-limit-factor(程序内对应 userLimitFactor)  
　表示一个用户可以最大获取资源的能力,可以获取当前队列容量乘以该值的最大资源，默认为 1 以确保无论集群负载如何 单用户都不能使用超过队列容量的资源。体现在源码中  

```
　Resources.multiplyAndRoundDown(queueCapacity, userLimitFactor)
```

[LeafQueue.java]

```
private Resource computeUserLimit(FiCaSchedulerApp application,
      Resource clusterResource, Resource required, User user,
      Set<String> requestedLabels) {
    Resource queueCapacity = Resource.newInstance(0, 0);
    // What is our current capacity?
    // * It is equal to the max(required, queue-capacity) if
    //   we're running below capacity. The 'max' ensures that jobs in queues
    //   with miniscule capacity (< 1 slot) make progress
    // * If we're running over capacity, then its
    //   (usedResources + required) (which extra resources we are allocating)
    // 第一步: 依据标签计算队列的 capacity
    if (requestedLabels != null && !requestedLabels.isEmpty()) {
     String firstLabel = requestedLabels.iterator().next();
      queueCapacity =
          Resources
              .max(resourceCalculator, clusterResource, queueCapacity,
                  Resources.multiplyAndNormalizeUp(resourceCalculator,
                      labelManager.getResourceByLabel(firstLabel,
                          clusterResource),
                      queueCapacities.getAbsoluteCapacity(firstLabel),
                      minimumAllocation));
    } else {
      queueCapacity =
          Resources.multiplyAndNormalizeUp(resourceCalculator, labelManager
                .getResourceByLabel(CommonNodeLabelsManager.NO_LABEL, clusterResource),
              queueCapacities.getAbsoluteCapacity(), minimumAllocation);
    }
    queueCapacity =
        Resources.max(
            resourceCalculator, clusterResource,
            queueCapacity,
            required);
    // 第二步: 计算 currentCapacity
    Resource currentCapacity =
        Resources.lessThan(resourceCalculator, clusterResource,
            queueUsage.getUsed(), queueCapacity) ?
            queueCapacity : Resources.add(queueUsage.getUsed(), required);

    // Never allow a single user to take more than the
    // queue's configured capacity * user-limit-factor.
    // Also, the queue's configured capacity should be higher than
    // queue-hard-limit * ulMin

    final int activeUsers = activeUsersManager.getNumActiveUsers();
    // 第三步: 计算用户可使用的最高容量
    Resource limit =
        Resources.roundUp(
            resourceCalculator,
            Resources.min(
                resourceCalculator, clusterResource,
                Resources.max(
                    resourceCalculator, clusterResource,
                    //当前容量/活跃用户数
                    Resources.divideAndCeil(
                        resourceCalculator, currentCapacity, activeUsers),
                    //当前容量 * 最高用户数/100
                    Resources.divideAndCeil(
                        resourceCalculator,
                        Resources.multiplyAndRoundDown(
                            currentCapacity, userLimit),
                        100)
                    ),
                //当前容量 * 用户限制因子(倍数)
                Resources.multiplyAndRoundDown(queueCapacity, userLimitFactor)
                ),
            minimumAllocation);
    .....
    user.setUserResourceLimit(limit);
    return limit;
  }
```
## 测试场景
测试项: 用户提交应用时 user headroom 计算方式及能否平分集群资源。  
用户限制因子 userLimitFactor = 100，queueCapacity * userLimitFactor >> clusterResource，即单用户资源上限很高  
用户限制 userLimit = 30，即用户可以使用队列[0,max{0.3,1/activeUser}]资源  

###  scene 1 单用户单队列单应用可以独占集群资源
单用户时 user headroom 的变化 随着常规调度模块为应用不断分配资源(可用集群全部资源) 分为(分段函数)两个阶段:    
1. 当 used < queueCapacity 时，limit = queueCapacity  
2. 当 used >= queueCapacity 时，limit = used + required。上限是 min{queueCapacity * userLimitFactor, clusterResource}  

单用户时 user headroom 变化曲线如下:  

![](/img/pictures/yarn_headroom/headroom_1.jpeg)

### scene 2 单用户双队列双应用未必平分集群资源 
基于 scene 1,随后在另外一个队列(Queue\_B)提交一个新的 APP(M 个 N 大小的容器)。该用户在 Queue\_B 的 headroom 可分为两个阶段:  
1. 当 used < queueCapacity 时,limit = queueCapacity。此时抢占调度模块 Queue\_B 的 堆积(pending)ResourceRequest = min{limit-used, M * N}。抢占模块会负责将 overCapacity 队列的资源释放，Queue\_B 队列可以有资源运行。即 Queue\_B 可以运行容器,used 值逼近 queueCapacity，pendingResourceRequest 值逼近于 0(虽然此时 APP 还有很多容器没有分配)。  
2.  这个阶段有两种可能，关键在于: 常规调度模块能否利用集群剩余资源为 Queue\_B 再分配一个容器，使其 used > queueCapacity。  
　　2a.不能分配一个容器。limit = queueCapacity，pendingResourceRequest = min{limit - used,M * N} 趋近于 0。pendingResourceRequest 会触发抢占调度(不考虑 0.2 的deadzone,超过 capacity * (1+deadzone)的 overCapacity 队列才会真正还回资源)。Queue\_B 欠分配，最终理想容量为 used + pendingResourceRequest(因为很小,且初始 idealAssigned/guarantee 小,会先被分配)，pendingResourceRequest(min{limit-used,M * N}) 的资源量不足以启动一个容器(原因很多,尤其是大容器,抢占的资源分布在繁忙集群的多个节点上,每个节点都不足以启动一个容器；pendingResourceRequest 被 limit 削弱过,不足以启动一个容器)。此时 user 在 Queue_B 的 headroom 恒等于 queueCapacity。抢占也不能抢回足够资源。          
　　2b. 能分配一个容器，使队列 used > queueCapacity。此时队列的 currentCapacity 值由 queueCapacity(常量)跃迁至 used+required，提高了 headroom 上限,进而提高了 pendingResourceRequest 上限，抢占调度依据 pendingResourceRequest 值计算集群各队列的理想容量，抢回了资源足以启动容器，又提升了 used。剩下限制 APP 资源量的条件只有 min{queueCapacity * userLimitFactor, maxCapacity,平分 clusterResource}了。

单用户双队列双 APP 的 user headroom 变化曲线如下:  

![](/img/pictures/yarn_headroom/headroom_2.jpeg)

### scene 3 多用户多队列应用更难平分集群资源 
 由上述源码可知,limit 取值为 min{queueCapacity * userLimitFactor, max{currentCapacty/activeUser, currentCapacity * userLimit/100}}    
 其中 activeUser 为整个系统的活跃用户数，即多用户下 limit 取值比单用户更低，但是分段函数的跃迁规则是不变的(used > capacity?)，即系统既要为APP 分配容器，而且容器大小要使队列 used 资源量从 queueCapcity/activeUser 或者 queueCapacity * userLimit/100 跃迁至超过 queueCapacity。
 
## 一点思考  
1. 设置 userLimitFactor 很大时(超过 clusterResource),实现多用户平分集群资源是不现实的。第一个提交的大应用将会先独占集群资源，后续提交的应用借助抢占调度获得部分资源，但资源上界一般不会超过队列的容量，越来越难平分资源。但是如果缩小 userLimitFactor,使其不超过 clusterResource的某阈值，其他队列跃迁的可能性增加，通过抢占调度是可实现多用户平分资源。     
2. user headroom 是为了解决在多用户环境下限制用户可使用资源量,为每个用户维护的瞬时状态量，通过 userLimit 和 userLimitFactor 保证了用户可使用资源的上界。  
3. user headroom 的计算方式: min{queueCapacity * userLimitFactor, max{currentCapacity/activeUser, currentCapacity * userLimit/100}}         
4. Yarn 的三大调度方式: 常规调度/抢占调度/预订调度 对于大容器都是不友好的。  

 
