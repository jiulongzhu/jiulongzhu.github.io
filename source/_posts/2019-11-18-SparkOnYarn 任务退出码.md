---
layout:     post
title:      Spark On Yarn 任务退出码
subtitle:   Spark On Yarn  ExitCode
date:       2019-11-18
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - Yarn 2.7.3
    - Spark 2.3.0
    - Spark On Yarn
---
## 概述

SparkOnYarn 程序的退出码种类比较繁多逻辑杂乱，主要包括：   

1. Yarn 退出码      
2. Spark 退出码    
3. JVM 退出码   

其中一些退出码可能是多个环节共同作用最终展现到日志上，e.g. SparkOnYarn 程序封装 driver/executor 的 ContainerLauchContext 时启动命令时加入"-XX:OnOutOfMemoryError=kill %p"，所以当程序 OOM 使用默认 signal(15) 终止 Container 脚本启动的 JVM，上报到 SparkOnYarn 日志的退出码是 143(128+15)。 

<!-- more -->

以下均基于 Hadoop2.7.3，Spark 2.3-SNAPSHOT，jdk1.8 

## Yarn 退出码

Yarn container 的退出码分为两类: container-executor 和 container。container 是 Yarn 资源调度的基础，也是资源隔离的基础，在同一台服务器上运行的多个container 之间内存、CPU和权限等方面不应该相互影响(实际上内存和 CPU 没有做到绝对的资源隔离,仅仅监控内存/不管 CPU,在此不细说)。container-executor 负责使用 Yarn 参考 ContainerLaunchContext 上下文环境组装的脚本 初始化、启动和终止 Container，因此其错误码体系更像是操作系统的进程退出码。container-executor 共分两类: 通用的 DefaultContainerExecutor，所有的 Container 的用户均为启动 NodeManager 的用户；LinuxContainerExecutor，每个 Container 进程可以由不同用户启动，并支持 CGROUP 和 ACL。container 错误码体系适用于所有容器，是 Yarn 框架的一部分，应用程序错误码体系的补充。即 container-executor 错误码体系更靠近操作系统，container 错误码体系更靠近应用程序。  

### container-executor

仅列出 LinuxContainerExecutor 错误码体系  

| 错误码  | 错误信息  | 描述 |
|:------------- |:---------------|:-------------|
|1|INVALID\_ARGUMENT\_NUMBER|1.启动脚本给定输入参数数量不符 2.未能成功初始化|
|2|INVALID\_USER\_NAME|启动脚本所属用户不存在|
|3| INVALID\_COMMAND\_PROVIDED |无法识别被提供的启动命令|
|4|SUPER\_USER\_NOT\_ALLOWED\_TO\_RUN\_TASKS|未启用|
|5|INVALID\_NM\_ROOT\_DIRS|启动脚本参数定义的 NM root 目录不存在|
|6|SETUID\_OPER\_FAILED|无法设置 UID 或 GID|
|7|UNABLE\_TO\_EXECUTE\_CONTAINER\_SCRIPT|无法运行启动脚本|
|8|UNABLE\_TO\_SIGNAL\_CONTAINER|无法向指定容器发送信号|
|9|INVALID\_CONTAINER\_PID|设置的启动命令 PID 不大于 0或不合法|
|10| ERROR\_RESOLVING\_FILE\_PATH |未启用|
|11| RELATIVE\_PATH\_COMPONENTS\_IN\_FILE\_PATH |未启用|
|12| UNABLE\_TO\_STAT\_FILE |未启用|
|13| FILE\_NOT\_OWNED\_BY\_ROOT |未启用|
|14| PREPARE\_CONTAINER\_DIRECTORIES\_FAILED |未启用|
|15| INITIALIZE\_CONTAINER\_FAILED |未启用|
|16| PREPARE\_CONTAINER\_LOGS\_FAILED |未启用|
|17| INVALID\_LOG\_DIR |未启用|
|18| OUT\_OF\_MEMORY |启动命令未能获得足够的内存|
|19| INITIALIZE\_DISTCACHEFILE\_FAILED |未启用|
|20| INITIALIZE\_USER\_FAILED |无法获取用户的 NM 目录|
|21| UNABLE\_TO\_BUILD\_PATH|启动脚本创建路径失败|
|22| INVALID\_CONTAINER\_EXEC\_PERMISSIONS |启动脚本没有设置正确的执行权限|
|23| PREPARE\_JOB\_LOGS\_FAILED |未启用|
|24| INVALID\_CONFIG\_FILE |container-executor.cfg文件不存在或者权限不正确|
|25| SETSID\_OPER\_FAILED|设置容器的 SID(sessionId?)失败|
|26| WRITE\_PIDFILE\_FAILED|无法将 PID 写入 PID 文件|
|27| WRITE\_CGROUP\_FAILED|无法写入CGROUP 信息(/sys/fs/cgroup ?)|
|其他|UNKNOWN\_ERROR|需要依据错误信息具体分析|

以上主要参考 hadoop-project/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server-nodemanager/src/main/native/container-executor/impl/container-executor.h   

### container

LinuxContainerExecutor 和 DefaultContainerExecutor 通用的错误码体系，Yarn 框架的一部分，可作为应用程序错误码的补充。  

| 错误码  | 错误信息  | 描述 |
|:------------- |:---------------|:-------------|
|0|SUCCESS|容器成功退出|
|-1000|INVALID|容器退出码的初始值|
|-100|ABORTED|容器被框架杀死,可能是 app 释放容器或 nm 失败丢失容器导致|
|-101|DISKS\_FAILED|NM 本地目录数量超过阈值|
|-102|PREEMPTED|容器被抢占|
|-103|KILLED\_EXCEEDED\_VMEM|虚拟内存超限被杀|
|-104|KILLED\_EXCEEDED\_PMEM|物理内存超限被杀|
|-105|KILLED\_BY\_APPMASTER|被 am 请求终止|
|-106|KILLED\_BY\_RESOURCEMANAGER|被 rm 请求终止|
|-107|KILLED\_AFTER\_APP\_COMPLETION|app 完成后被杀死|

以上主要参考 org.apache.hadoop.yarn.api.records.ContainerExitStatus.java  

### Application  

通用的 Yarn Application 状态码定义  

| 错误码  | 错误信息  | 描述 |
|:------------- |:---------------|:-------------|
||UNDEFINED|APP 未完成时的状态码(初始值)|
||SUCCEEDED|APP 成功完成|
||FAILED|APP 失败|
||KILLED|APP 被提交用户或者管理员用户终止|

以上参考 org.apache.hadoop.yarn.api.records.FinalApplicationStatus.java  

## Spark 退出码

### Executor 退出码

| 错误码  | 错误信息  | 描述 |
|:------------- |:---------------|:-------------|
|50|UNCAUGHT_EXCEPTION|触发了默认的异常处理器(?)|
|51|UNCAUGHT\_EXCEPTION\_TWICE|触发了默认的异常处理器,并在记录异常时触发了新的异常(?)|
|52|OOM|触发了默认的异常处理器,且异常是 OutOfMemoryError|
|53|DISK\_STORE\_FAILED\_TO\_CREATE\_DIR|DiskStore 未能创建本地临时目录|
|54|EXTERNAL\_BLOCK\_STORE\_FAILED\_TO\_INITIALIZE|ExternalBlockStore 未能初始化|
|55|EXTERNAL\_BLOCK\_STORE\_FAILED\_TO\_CREATE\_DIR|ExternalBlockStore 未能创建本地临时目录|
|56|HEARTBEAT_FAILURE|executor 向 driver 发送心跳信号失败次数超过了 spark.executor.heartbeat.maxFailures|

以上主要参考 org.apache.spark.executor.ExecutorExitCode.scala  

### ApplicationMaster 退出码

| 错误码  | 错误信息  | 描述 |
|:------------- |:---------------|:-------------|
|0|EXIT_SUCCESS||
|10|EXIT_UNCAUGHT_EXCEPTION|未捕获的异常(此列表之外的异常)退出|
|11|EXIT_MAX_EXECUTOR_FAILURES|失败的 executor 数量超过目标值。目标值优先取 spark.yarn.max.executor.failures(默认无),未配置 且 开启动态资源管理时取spark.dynamicAllocation.maxExecutors 的 2 倍,其他情况则取 spark.executor.instances 的 2 倍|
|12|EXIT_REPORTER_FAILURE|应用进度汇报线程(progress reporter)的失败次数超过spark.yarn.scheduler.reporterThread.maxFailures(默认 5)|
|13|EXIT_SC_NOT_INITED|SparkContext 初始化时间超过 spark.yarn.am.waitTime(默认 100s)|
|14|EXIT_SECURITY|未启用|
|15|EXIT_EXCEPTION_USER_CLASS|用户MainClass 抛出异常|
|16|EXIT_EARLY|在 APP 结束前调用了 ShutdownHook|

以上参考 org.apache.spark.deploy.yarn.ApplicationMaster.scala  

## JVM 退出码

数值上超过 128 的退出码很有可能是由 Unix Signal 触发的程序关闭导致的，可以通过用退出码减去 128来计算 Unix Signal。例如最常见的 137 退出码是由 kill -9 强制杀死抛出的，143 退出码一般是由 kill 或 kill -15 杀死抛出的。 

| 错误码  | 错误信息  | 描述 |
|:------------- |:---------------|:-------------|
|137|FORCE_KILLED|128+9,即是由 kill -9 命令强制终止,一般是虚拟或物理内存超限导致、也有可能是操作系统整体内存紧张杀死进程|
|143|TERMINATED|128+15,即是由 kill 命令终止的,一般是 OOM/FGC 导致|
|154|LOST|128+27,虚拟计时器过期(?)|

```
Then I ran the program in one terminal window (java Death; echo $?) while iterating through all kill signals (0-31) in another:
kill -$SIGNAL $(jps | grep Death | cut -d\  -f1)

signal	    shutdown runs hook exit code    comment
default (15)	yes	yes	143	SIGTERM is the default unix kill signal
0	no	-	-	
1 (SIGHUP)	yes	yes	129	
2 (SIGINT)	yes	yes	130	SIGINT is the signal sent on ^C
3 (SIGQUIT)	no	-	-	Makes the JVM dump threads / stack-traces
4 (SIGILL)	yes	no	134	Makes the JVM write a core dump and abort on trap 6
5	yes	no	133	Makes the JVM exit with "Trace/BPT trap: 5"
6 (SIGABRT)	yes	no	134	Makes the JVM exit with "Abort trap: 6"
7	yes	no	135	Makes the JVM exit with "EMT trap: 7"
8 (SIGFPE)	yes	no	134	Makes the JVM write a core dump and abort on trap 6
9 (SIGKILL)	yes	no	137	The JVM is forcibly killed (exits with "Killed: 9")
10 (SIGBUS)	yes	no	134	Emulates a "Bus Error"
11 (SIGSEGV)	yes	no	134	Emulates a "Segmentation fault"
12	yes	no	140	Makes the JVM exit with "Bad system call: 12"
13	no	-	-	
14	yes	no	142	Makes the JVM exit with "Alarm clock: 14"
15 (SIGTERM)	yes	yes	143	This is the default unix kill signal
16	no	-	-	
17	no	-	145	Stops the application (sends it to the background), same as ^Z
18	no	-	146	Stops the application (sends it to the background), same as ^Z
19	no	-	-	
20	no	-	-	
21	no	-	149	Stops the application (sends it to the background), same as ^Z
22	no	-	150	Stops the application (sends it to the background), same as ^Z
23	no	-	-	
24	yes	no	152	Makes the JVM exit with "Cputime limit exceeded: 24"
25	no	-	-	
26	yes	no	154	Makes the JVM exit with "Virtual timer expired: 26"
27	yes	no	155	Makes the JVM exit with "Profiling timer expired: 27"
28	no	-	-	
29	no	-	-	
30	yes	no	158	Makes the JVM exit with "User defined signal 1: 30"
31	yes	no	134	Makes the JVM exit on Segmentation fault


This list was compiled using (a quite old) Oracle Hotspot Java 8 EA on Mac OS X:
java version "1.8.0-ea"
Java(TM) SE Runtime Environment (build 1.8.0-ea-b65)
Java HotSpot(TM) 64-Bit Server VM (build 25.0-b09, mixed mode)
```

以上主要参考 http://journal.thobe.org/2013/02/jvms-and-kill-signals.html

## 参考
1. hadoop-project/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server-nodemanager/src/main/native/container-executor/impl/container-executor.h   
2. org.apache.hadoop.yarn.api.records.ContainerExitStatus.java    
3. org.apache.spark.executor.ExecutorExitCode.scala  
4. http://journal.thobe.org/2013/02/jvms-and-kill-signals.html  
5. org.apache.hadoop.yarn.server.nodemanager.ExitCode.java
6. org.apache.spark.deploy.yarn.ApplicationMaster.scala  
7. org.apache.hadoop.yarn.api.records.FinalApplicationStatus.java  
