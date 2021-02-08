---
layout:     post
title:      Linux 服务器分析 JVM Dump 文件
date:       2020-04-08
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - tools
---

JVM Dump 文件传输到本地分析，通常需要等待较长时间，且本地环境配置不支持分析大内存 JVM 的 Dump 文件。因此合理的方式是使用 Eclipse 的 MAT(MemoryAnalyzeTool) 工具在服务器上分析 Dump 文件结束后将结果传输到本地查看。    

<!-- more -->

### 环境要求  

JDK 1.8.0 以上  
Linux 操作系统    
MAT 工具(https://www.eclipse.org/mat/downloads.php)    

### 配置 MAT 

```
unzip MemoryAnalyzer-*-linux.gtk.*.zip 

vim MemoryAnalyzer.ini 
## 修改-Xmx，通常设置的比要分析的 JVM Xmx 略大即可  
```

### JVM Dump 

```
jmap -dump:format=b,file=PID.dump PID
```

>> 
1. 如果需要 Dump Docker 下的进程，需要进入到 Docker 内找到PID，dump 到 Docker 内然后复制 Dump 文件到宿主机 
2. 如果遇到了 Error attaching to core file 错误,可使用 su -m USER -c "jmap -dump:format=b,file=PID.dump PID" 命令来解决    

### 分析 Dump

```
cd mat  
sh ParseHeapDump.sh PID.dump  org.eclipse.mat.api:suspects org.eclipse.mat.api:overview org.eclipse.mat.api:top_components
```  

会产生 3 个 zip 文件: PID_Leak_Suspects.zip,PID_System_Overview.zip,PID_Top_Components.zip，即(疑似)内存泄露分析、系统内存使用概览、GC-ROOT 信息。 

### 查看结果  

将三个 zip 文件传输到本地，解压后在浏览器打开 index.html 即可。  

	
### 参考 

https://www.cnblogs.com/trust-freedom/p/6744948.html   
http://moheqionglin.com/site/blogs/24/detail.html  
