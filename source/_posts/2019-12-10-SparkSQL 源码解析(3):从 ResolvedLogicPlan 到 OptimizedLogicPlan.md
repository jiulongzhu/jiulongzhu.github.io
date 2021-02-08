---
layout:     post
title:     SparkSQL源码解析(3):从 ResolvedLogicPlan 到 OptimizedLogicPlan
subtitle:   SparkSQL 源码解析
date:       2019-12-10
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - Spark 2.3.0
    - Spark SQL
    - 源码解析
---


## 概述  

Parser 模块负责将 SQL 文本解析为 UnResolvedLogicPlan，Analyzer 模块负责将 UnResolvedLogicPlan 绑定解析为 ResolvedLogicPlan。Rule-Based Optimizer，简称 RBO 或 Optimizer 模块负责基于规则优化逻辑计划，主要思路是基于各类型规则对 ResolvedLogicPlan 进行优化达到减少每个逻辑计划树节点输入数据量或计算代价的目的，大名鼎鼎的列裁剪和谓词下推、常量折叠就是出自于 Optimizer 模块。      

<!-- more -->

## 源码解析  

[QueryExecution.scala]

```
// 第一步: 使用 CacheManager 替换逻辑计划为已解析过的逻辑计划
lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    sparkSession.sharedState.cacheManager.useCachedData(analyzed)
  }
 // 第二步: 基于规则优化逻辑计划树
  lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)
```

### CacheManager 

[CacheManager.scala]

```
private val cachedData = new java.util.LinkedList[CachedData]

def useCachedData(plan: LogicalPlan): LogicalPlan = {
    // 先序遍历
    val newPlan = plan transformDown {
      case hint: ResolvedHint => hint
      case currentFragment =>
        // 检索 CacheManager
        lookupCachedData(currentFragment)
           // 使用当前计划的输出字段替换原有的
          .map(_.cachedRepresentation.withOutput(currentFragment.output))
          .getOrElse(currentFragment)
    }
    newPlan transformAllExpressions {
      // 剥去子查询别名外壳
      case s: SubqueryExpression => s.withNewPlan(useCachedData(s.plan))
    }
  }
  
 def lookupCachedData(plan: LogicalPlan): Option[CachedData] = readLock {
    // 遍历对比CacheManager 中是否存在和当前计划一致的  
    cachedData.asScala.find(cd => plan.sameResult(cd.plan))
} 
```
通常判断两个查询计划是否相同是很复杂的问题，因此可能实际相同也可以返回 false，但是实际不同一定不能返回 true。这种行为不会影响正确性，仅仅相当于弃用了 CacheManager，多做了一些重复处理而已。这种思想有点像 BloomFilter 的误判率: 函数判断在集合但是实际上可能不在，仅仅是多个 hash 函数得到的位置在 bitmap 中恰好非 0；返回不在集合时一定不在。  
[QueryPlan.scala]

```
// 将逻辑计划规范化之后再做比较
final def sameResult(other: PlanType): Boolean = this.canonicalized == other.canonicalized
// 规范化 逻辑计划涉及的所有字段的 id(从 0 递增)。
protected def doCanonicalize(): PlanType = {
    val canonicalizedChildren = children.map(_.canonicalized)
    var id = -1
    mapExpressions {
      case a: Alias =>
        id += 1
        val normalizedChild = QueryPlan.normalizeExprId(a.child, allAttributes)
        Alias(normalizedChild, "")(ExprId(id), a.qualifier)

      case ar: AttributeReference if allAttributes.indexOf(ar.exprId) == -1 =>
        id += 1
        ar.withExprId(ExprId(id)).canonicalized

      case other => QueryPlan.normalizeExprId(other, allAttributes)
    }.withNewChildren(canonicalizedChildren)
  }
```
有必要提一下 InMemoryRelation 这个类，这是 CacheManager 缓存已被 RBO 优化过的内存数据结构，包括：逻辑计划的输出字段，是否压缩(spark.sql.inMemoryColumnarStorage.compressed，默认 true，SparkSQL 将根据元数据信息自动匹配编码器)，批处理大小(spark.sql.inMemoryColumnarStorage.batchSize，默认 10000，控制列式存储的批处理大小，较大的值能提高内存利用率和压缩率 但是读数据时可能会 OOM)，存储等级(默认 MEMORY_AND_DISK)，RBO 优化完成的物理计划 SparkPlan，表名，对应 RDD[CachedBatch]，物理数据字节数，额外有一个<b> statsOfPlanToCache 暂时不知用途</b>  
[InMemoryRelation.scala]

```
case class InMemoryRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    batchSize: Int,
    storageLevel: StorageLevel,
    @transient child: SparkPlan,
    tableName: Option[String])(
    @transient var _cachedColumnBuffers: RDD[CachedBatch] = null,
    val sizeInBytesStats: LongAccumulator = child.sqlContext.sparkContext.longAccumulator,
    statsOfPlanToCache: Statistics)
  extends logical.LeafNode with MultiInstanceRelation
```
CachedBatch 用以表明 batch 中数据行数，序列化的列数据 buffer，数据的元数据类型。  
[CachedBatch.scala]

```
case class CachedBatch(numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow)
```

### RBO
SessionState 初始化时指定的 Optimizer 为 SparkOptimizer，SparkOptimizer 继承自 Optimizer，Optimizer 与 Analyzer 均继承自 RuleExecutor，都使用RuleExecutor#execute 方法遍历逻辑计划树应用 Optimizer/Analyzer 各自指定的规则库和执行策略 优化每个逻辑计划。故不再引用 RuleExecutor#execute 方法。       
[BaseSessionStateBuilder.scala]  

```
protected def optimizer: Optimizer = {
    new SparkOptimizer(catalog, experimentalMethods) {
      override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] =
        super.extendedOperatorOptimizationRules ++ customOperatorOptimizationRules
    }
  }
```
除了类似于前后拦截器功能的 preOptimizationBatches、postHocOptimizationBatches 和拓展规则之外，SparkOptimizer 主要引用了 Optimizer 定义的通用规则，还有 4 个自定义规则:  

1.  OptimizeMetadataOnlyQuery，优化那些只遍历表分区键级别的元数据就可以完成的逻辑计划   　
	* 在分区键上做聚合。e.g. SELECT col FROM tbl GROUP by col;   
	* 在分区键上去重并使用聚合函数。e.g. SELECT col1,count(DISTINCT col2) FROM tbl GROUP BY col1;   
	* 在分区键上应用有去重功能的聚合函数。e.g. SELECT col1, Max(col2) FROM tbl GROUP BY col1;   
2. ExtractPythonUDFFromAggregate，提取聚合操作中所有 PythonUDF。十分不推荐使用 pyspark   
3. PruneFileSourcePartitions，物理文件分区下推。 读取 hadoop 目录时尽可能将过滤条件下推到分区键上，避免扫描所有文件。   
4. PushDownOperatorsToDataSource  过滤操作下推到数据源，以提高性能。  

[SparkOptimizer.scala]

```
override def batches: Seq[Batch] = (preOptimizationBatches ++ super.batches :+
    Batch("Optimize Metadata Only Query", Once, OptimizeMetadataOnlyQuery(catalog)) :+
    Batch("Extract Python UDF from Aggregate", Once, ExtractPythonUDFFromAggregate) :+
    Batch("Prune File Source Table Partitions", Once, PruneFileSourcePartitions) :+
    Batch("Push down operators to data source scan", Once, PushDownOperatorsToDataSource)) ++
    postHocOptimizationBatches :+
    Batch("User Provided Optimizers", fixedPoint, experimentalMethods.extraOptimizations: _*)
```
[Optimizer.scala]

```
def batches: Seq[Batch] = {
    val operatorOptimizationRuleSet =
      Seq(
        // Operator push down
        PushProjectionThroughUnion,
        ReorderJoin,
        EliminateOuterJoin,
        PushPredicateThroughJoin,
        PushDownPredicate,
        LimitPushDown,
        ColumnPruning,
        InferFiltersFromConstraints,
        // Operator combine
        CollapseRepartition,
        CollapseProject,
        CollapseWindow,
        CombineFilters,
        CombineLimits,
        CombineUnions,
        // Constant folding and strength reduction
        NullPropagation,
        ConstantPropagation,
        FoldablePropagation,
        OptimizeIn,
        ConstantFolding,
        ReorderAssociativeOperator,
        LikeSimplification,
        BooleanSimplification,
        SimplifyConditionals,
        RemoveDispensableExpressions,
        SimplifyBinaryComparison,
        PruneFilters,
        EliminateSorts,
        SimplifyCasts,
        SimplifyCaseConversionExpressions,
        RewriteCorrelatedScalarSubquery,
        EliminateSerialization,
        RemoveRedundantAliases,
        RemoveRedundantProject,
        SimplifyCreateStructOps,
        SimplifyCreateArrayOps,
        SimplifyCreateMapOps,
        CombineConcats) ++
        extendedOperatorOptimizationRules

    val operatorOptimizationBatch: Seq[Batch] = {
      val rulesWithoutInferFiltersFromConstraints =
        operatorOptimizationRuleSet.filterNot(_ == InferFiltersFromConstraints)
      Batch("Operator Optimization before Inferring Filters", fixedPoint,
        rulesWithoutInferFiltersFromConstraints: _*) ::
      Batch("Infer Filters", Once,
        InferFiltersFromConstraints) ::
      Batch("Operator Optimization after Inferring Filters", fixedPoint,
        rulesWithoutInferFiltersFromConstraints: _*) :: Nil
    }

    (Batch("Eliminate Distinct", Once, EliminateDistinct) ::
    // 译: 从技术上将，”Finish Analysis“中的部分规则不是 optimizer rule 而是 analyzer rule，因为它们是保证正确性必须的。
    // 但是 因为我们使用 analyzer 来规范化查询(视图定义)，我们在 analyzer 中不去除子查询或计算当前时间
    Batch("Finish Analysis", Once,
      EliminateSubqueryAliases,
      EliminateView,
      ReplaceExpressions,
      ComputeCurrentTime,
      GetCurrentDatabase(sessionCatalog),
      RewriteDistinctAggregates,
      ReplaceDeduplicateWithAggregate) ::
    //////////////////////////////////////////////////////////////////////////////////////////
    // Optimizer rules start here
    //////////////////////////////////////////////////////////////////////////////////////////
      // 译: 在应用主要的优化规则之前先调用 CombineUnions,因为可以减少迭代次数，而其他规则可以在两个相邻的 Union操作符之间添加/移动额外的操作符
      // 在规则库"Operator Optimizations"中再次调用 CombineUnions,是因为其他操作符可能会导致两个单独的 Union 变得相邻。
    Batch("Union", Once,
      CombineUnions) ::
    Batch("Pullup Correlated Expressions", Once,
      PullupCorrelatedPredicates) ::
    Batch("Subquery", Once,
      OptimizeSubqueries) ::
    Batch("Replace Operators", fixedPoint,
      ReplaceIntersectWithSemiJoin,
      ReplaceExceptWithFilter,
      ReplaceExceptWithAntiJoin,
      ReplaceDistinctWithAggregate) ::
    Batch("Aggregate", fixedPoint,
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions) :: Nil ++
    operatorOptimizationBatch) :+
    Batch("Join Reorder", Once,
      CostBasedJoinReorder) :+
    Batch("Decimal Optimizations", fixedPoint,
      DecimalAggregates) :+
    Batch("Object Expressions Optimization", fixedPoint,
      EliminateMapObjects,
      CombineTypedFilters) :+
    Batch("LocalRelation", fixedPoint,
      ConvertToLocalRelation,
      PropagateEmptyRelation) :+
    // The following batch should be executed after batch "Join Reorder" and "LocalRelation".
    Batch("Check Cartesian Products", Once,
      CheckCartesianProducts) :+
    Batch("RewriteSubquery", Once,
      RewritePredicateSubquery,
      ColumnPruning,
      CollapseProject,
      RemoveRedundantProject)
  }
```

按规则(库)应用的顺序串行列出(下述规则均在 SparkOptimizer中规则之前)，<b>欢迎指出错误</b>       

|规则库名 |规则类名  |功能 |备注|
|:------------- |:---------------|:-------------|:-------------|
|Eliminate Distinct|EliminateDistinct|去除 MAX/MIN 函数内的 DISTINCT|SELECT MAX(DISTINCT(age)) FROM a|
|Finish Analysis|EliminateSubqueryAliases|去除子查询别名,子查询仅仅提供查询属性集的作用,analyzer 阶段结束后便可去除子查询别名|Finish Analysis 规则不在 analyzer 中删除的意义是保存原始的 analyzed logic plan|
|Finish Analysis|EliminateView|去除视图操作符|视图的输出属性集必须和子查询输出属性集完全一致|
|Finish Analysis|ReplaceExpressions|将RuntimeReplaceable 表达式替换为可执行表达式|一般用来兼容各类数据库，例如用 coalesce 替换 nvl 函数|
|Finish Analysis|ComputeCurrentTime|记录当前时间,SQL 中任意位置和执行顺序的操作时间(date,timestamp)都会返回一致的结果||
|Finish Analysis|GetCurrentDatabase|使用 SessionCatalog 的数据库作为当前数据库返回|CurrentDataBase 函数没必要在执行阶段一条条计算,在 RBO 阶段获取完作为常量替换了函数表达式|
|Finish Analysis|RewriteDistinctAggregates|重写 count(distinct xx),展开数据通过一次聚合即可算出结果|SELECT count(distinct name) as name_cnt, count(distinct(age)) as age_cnt,sum(age) as age_sum FROM p GROUP BY year=> 将数据展开成[[year,null,null,type0,ageVal],[year,name,null,type1,null],[year,null,age,type2,ageVal]]],group by (year,name,age,type,age),sum(if(type1,ageVal,0));sum(if(type2,1,0));sum(if(type3,1,0))...|
|Finish Analysis|ReplaceDeduplicateWithAggregate|替换 Deduplicate 操作为 Aggregate|没找到 Deduplicate 对应的语法,FIX ME|
|Union|CombineUnions|将有父子关系的 union all 汇集叶子节点一层|SELECT * FROM(SELECT * FROM a UNION ALL SELECT * FROM b) UNION ALL SELECT * FROM c => SELECT * FROM (a,b,c)|
|Pullup Correlated Expressions|PullupCorrelatedPredicates|?|?|
|Subquery|OptimizeSubqueries|RBO 优化子查询|子查询的父阶段为 SubQueryExpression,当解析到 SubQueryExpression 时,使用 RBO 优化子查询逻辑计划|
|Replace Operators|ReplaceIntersectWithSemiJoin|将 intersect 操作符转化为 left semi join 后再 distinct|left semi join 保留在右表中能关联到的左表选取列.SELECT name,age FROM a INTERSECT SELECT name,age FROM b => SELECT  DISTINCT name,age FROM a LEFT SEMI JOIN b ON a.name=b.name AND a.age=b.age|
|Replace Operators|ReplaceExceptWithFilter|将两个相同查询计划的子句 except操作符转为 Filter 后 Distinct|SELECT a1,a2 FROM t WHERE a2=12 EXCEPT SELECT a1,a2 FROM t WHERE a1=5  =>SELECT DISTINCT a1,a2 FROM t WHERE a2=12 AND NOT(a1=5)。需要相同查询计划子句才能执行此转换|
|Replace Operators|ReplaceExceptWithAntiJoin|不同查询计划的子句 except 操作符转为 left anti join 操作后 DISTINCT|SELECT a1,a2 FROM t1 EXCEPT SELECT b1,b2 FROM t2 => SELECT DISTINCT a1,a2 FROM t1 LEFT ANTI JOIN t2 ON t1.a1=t2.b1 AND t1.a2=t2.b2|
|Replace Operators|ReplaceDistinctWithAggregate|将 distinct 操作符转为 group by|SELECT DISTINCT a,b FROM t => SELECT a,b FROM t GROUP BY a,b|
|Aggregate|RemoveLiteralFromGroupExpressions|去除 group by 条件中的常量和可折叠常量表达式,不影响聚合结果但会减少 key 数据量| SELECT a,b FROM t GROUP BY a,b,1,1<0 =>SELECT a,b FROM t GROUP BY a,b|
|Aggregate |RemoveRepetitionFromGroupExpressions|去除 group by 条件中的重复表达式,不影响聚合结果但会减少 key 数据量|SELECT a,b FROM t GROUP BY a,b,b =>|
|Operator push down|PushProjectionThroughUnion|将查询列下推到各 union all 子句|SELECT a FROM (SELECT a,b FROM t1 UNION ALL SELECT c,d FROM t2) tbl_a => SELECT a FROM (SELECT a FROM t1 UNION ALL SELECT c FROM t2) tbl_a|
|Operator push down|ReorderJoin|按照逻辑计划数据行数启发式寻找星型模型的事实表和维度表,事实表在左以避免大表加载到内存,并以此重定义连接顺序|取决于spark.sql.cbo.starSchemaDetection 和 spark.sql.cbo.enabled,默认不开启。SELECT * FROM a,b,c where a.key=b.key and a.val=c.val => SELECT * FROM a join c on a.val=c.val join b on a.key=b.key , 大表居左小表居右|
|Operator push down|EliminateOuterJoin|尽可能转换 full join=>right/left join=>inner join|如果存在谓词可以削减 null-supplying 行(当输入数据为 null 时,谓词返回值为 null 或 false),则可以削弱 outer join. full join 可以转为 left/right/inner join,left/join join 可以转为 inner join. SELECT * FROM a left join b on a.key=b.key WHERE b.key IS NOT NULL =>SELECT * FROM a inner join b on a.key=b.key|
|Operator push down|PushPredicateThroughJoin|join 谓词下推|包含两层含义: where 条件下推到 join 条件,join 条件下推到子查询|
|Operator push down|PushDownPredicate|对于确定性的操作和谓词且谓词不能改变行的逻辑计划,尽可能下推谓词以减少输入数据量|对于支持类似 BloomFilter 的列式存储,有极大提升|
|Operator push down|LimitPushDown|旨在减少输入数据量。1. union all 上级和下级 limit 数值不同则下级取 min 2.left join 上级和下级 limit 数值不同则下级左侧limit 取 min;right join 同理|SELECT * FROM (SELECT * FORM a limit 20 UNION ALL SELECT * FORM b limit 15) temp LIMIT 10 或者 SELECT * FROM (SELECT * FROM a LEFT JOIN b ON a.key=b.key) temp LIMIT 10|
|Operator push down|ColumnPruning|列裁剪|去掉在查询/聚合/窗口函数/union 等用不到的列读取|
|Operator push down|InferFiltersFromConstraints|在子查询后或者关联后的约束条件中删掉子查询内或者关联子节点内已有的约束条件|在当前规则库会被过滤掉,在 Infer Filters 规则库执行|
|Operator combine|CollapseRepartition|折叠有父子关系的再分区操作|当父节点和子节点均为再分区操作,但父节点无 shuffle(coalesce api)子节点有 shuffle 且父节点再分区数大于子节点再分区数则删除父节点,若再分区数不大于子节点则不改变结构。若 shuffle 关系不为(false,true)及其他情况则折叠子节点 RepartitionOperation |
|Operator combine|CollapseProject|折叠有父子关系的父节点 Projection 操作|SELECT age FROM (SELECT name,age+1 as age FROM p) temp=>SELECT age+1 as age FROM p|
|Operator combine|CollapseWindow|折叠分区字段一致、排序方式一致且表达式相互独立、有父子关系的子节点 Window 操作||
|Operator combine|CombineFilters|折叠有父子关系的父节点 Filter 操作|SELECT name,age FROM (SELECT name,age FROM p where name is not null)temp where age>10 =>SELECT name,age FROM p WHERE name is not null AND age>10|
|Operator combine|CombineLimits|折叠有父子关系的父节点 Limit 操作,limit 数值取 minimum|Limit 分为 GlobalLimit,LocalLimit 两类,GlobalLimit 需要shuffle,LocalLimit 不需要。GlobalLimit(Union(A,B))可以转化为 GlobalLimit(Union(LocalLimit(A),LocalLimit(B)))来降低 shuffle 数据量|
|Operator combine|CombineUnions|重复规则,同 Union规则库的 CombineUnion 规则||
|Constant folding and strength reduction|NullPropagation|null 值替换|将表达式中可评估的 null 值替换为等效常量,count(name)=>count(1)|
|Constant folding and strength reduction|ConstantPropagation|Filter 操作中常量替换|SELECT * FROM p WHERE age=10 and age2=age+3 => SELECT * FROM p WHERE age=10 AND age2=13|
|Constant folding and strength reduction|FoldablePropagation|可折叠表达式替换|尽可能将属性替换为原始可折叠表达式,其他优化规则将利用可折叠表达式进行优化。SELECT 1.0 as x,'abc' as y,now() as z order by x,y,z => SELECT 1.0 as x,'abc' as y,now() as z ORDER BY 1.0,'abc',now(); 这样其他规则可以去掉 order by 操作符|
|Constant folding and strength reduction|OptimizeIn|IN 优化|尽可能优化 IN 谓词: 1.当 in 列表为空且指定列不可为空时直接返回 false 2.当 in 列表元素数量超过配置(默认 10),将列表转为 HashSet 以去重和使用哈希索引提高性能。In(value,seq[Literal])=>InSet(value,HashSet[Literal])|
|Constant folding and strength reduction|ConstantFolding|常量折叠|将可静态计算的表达式替换为等效常量.SELECT 1+2 as a FROM p=>SELECT 3 FROM p|
|Constant folding and strength reduction|ReorderAssociativeOperator|重排序所有整数类型运算符,将所有确定性整数折叠计算为一个结果|和 ConstantFolding 折叠不同，ConstantFolding 要在整个表达式都静态可计算(确定性)时才会应用。ReorderAssociativeOperator 这里尽管有不确定性部分，但会尽可能将确定性部分计算出来。SELECT (age+1)+2 as a FROM p =>SELECT age+3 as a FROM p|
|Constant folding and strength reduction|LikeSimplification|正则匹配简化|尽可能将 rlike正则匹配简化为字符串startWith,endWith,equal,container 等操作|
|Constant folding and strength reduction|BooleanSimplification|布尔表达式简化|尽可能简化 boolean 表达式/快速中断/删除不必要的 not 。false AND e=>false,true AND e=>e,a AND b=>Not(a).semanticEquals(b) => false,Not(Not(a))=>a|
|Constant folding and strength reduction|SimplifyConditionals|条件表达式简化(if(condition,trueVal,falseVal),case when)|如果 if表达式 恒定true或 false 则修改逻辑计划为相应的 val;如果 case when 表达式中有恒 false 值则删除其分支，若所有分支恒 false 则取 else 语句值；若 case when 第一个表达式恒 true，则取其值；恒 true 分支会删除其后续所有分支|
|Constant folding and strength reduction|RemoveDispensableExpressions|删除 UnaryPositive 节点(仅有标识子节点表达式作用)||
|Constant folding and strength reduction|SimplifyBinaryComparison|简化比较|1.将<=>替换为 true;2.将=和<=和>= 在两侧均非空且逻辑计划结果一致时替换为 true;3. 将>和<在两侧均非空且逻辑计划结果一致时替换为 false。age<AGE  =>false|
|Constant folding and strength reduction|PruneFilters|约束条件简化|1. 当约束条件恒 true 时,删除父节点;2.当约束条件恒 false 或 null 时,替换父节点的输入为空集;3.在父节点约束条件中去除子节点已有的约束条件|
|Constant folding and strength reduction|EliminateSorts|删除无效的排序|删除排序中确定性的排序方式,甚至不排序.SELECT name,age FROM p ORDER BY 1 ASC, age DESC|
|Constant folding and strength reduction|SimplifyCasts|强制类型转换简化|当强制转化的类型相同(仅限于 基础数据类型相同 或 Array 内嵌数据类型相同 或 Map 内嵌 key 和 value 类型均相同)时,去除 Cast (col to type) 操作|
|Constant folding and strength reduction|SimplifyCaseConversionExpressions|简化大小写转换表达式|内部转换会被外部转换覆盖 Upper(Lower(x))=>Upper(x),Lower(Lower(x))=>Lower(x)|
|Constant folding and strength reduction|RewriteCorrelatedScalarSubquery|？？||
|Constant folding and strength reduction|EliminateSerialization|删除不必要的在 object 和 InternalRow 之间的(循环)序列化/反序列化操作|1.反序列化为父节点,序列化为子节点且操作字段相同;2.将数据反序列化并追加数据到末尾时AppendColumns可替换AppendColumnsWithObject操作符 直接操作序列化后的数据(类似于 UnsafeSortShuffle) 3. TypedFilter为父节点,序列化为子节点时 4.反序列化为父节点,TypedFilter 为子节点时|
|Constant folding and strength reduction|RemoveRedundantAliases|删除无效别名|无效别名是指在子查询或者关联中不改变列名/列元数据的别名|
|Constant folding and strength reduction|RemoveRedundantProject|删除无效查询|无效查询是指父查询和子查询目标字段相同|
|Constant folding and strength reduction|SimplifyCreateStructOps|结构体创建简化|named_struct('name',name,'age',age).age=>age|
|Constant folding and strength reduction|SimplifyCreateArrayOps|数组创建简化|1.当对创建的数组按下标取值时,减少创建数组的数据量.Array(elem0,elem1...)(1)=>elems(1);2.Array(named_stuct(name,"nA"),named_struct(age,12))[0].name=>named_struct(name,"nA").name|
|Constant folding and strength reduction|SimplifyCreateMapOps|映射创建简化|map(key1->val2,key2->val2).key2 => case when key2 |
|Constant folding and strength reduction|CombineConcats|合并 concat|将有父子关系的 concat 所有子节点扁平化 合并|
|Infer Filters|InferFiltersFromConstraints|InferFiltersFromConstraints|在子查询后或者关联后的约束条件中删掉子查询内或者关联子节点内已有的约束条件|消除无效的约束传递造成的计算代价|
|Join Reorder|CostBasedJoinReorder|？|？|
|Decimal Optimizations|DecimalAggregates|加速浮点数运算|float 和 double 运算中一般需要控制精度(precision) 和小数位(scale)。窗口函数内聚合和普通聚合的 sum/avg 场景下将浮点计算转为长整形计算并在结束时转回来|
|Object Expressions Optimization|EliminateMapObjects|简化 MapObject 操作||
|Object Expressions Optimization|CombineTypedFilters|简化 TypedFilter|去除具有父子关系的子节点 TypedFilter,合并两者的约束条件|
|LocalRelation|ConvertToLocalRelation|简化为 LocalRelation|在 LocalRelation 上取 Limit 时,直接转化为 LocalRelation 在数据上取 Limit 减少输入数据数量|
|LocalRelation|PropagateEmptyRelation|空 Relation 优化|对于上述优化规则(e.g. 列裁剪谓词下推)产生的或基础数据为空的 Relation 进行优化. 1,关联时按照左右空 Relation 和Join 类型分别讨论,左空 && 左外连接=>Empty 2.union all 所有子节点都空直接返回空数据集 3.一元节点的所有子节点都是空 Relation 则直接返回空数据集合, Select/Limit/Repartition 且 children 都为空 Relation=>空数据集合|
|Check Cartesian Products|CheckCartesianProducts|笛卡尔积检测|检测逻辑计划树中是否有全外连接|
|RewriteSubquery|RewritePredicateSubquery|重写谓词子查询|将 in/exists 转为 semi join,将 not in / not exists 转为 anti join|
|RewriteSubquery|ColumnPruning|列裁剪|同 Aggregate规则库中的 ColumnPruning,去掉在查询/聚合/窗口函数/union 等用不到的列读取|
|RewriteSubquery|CollapseProject|折叠有父子关系的父节点 Projection 操作|同 Aggregate 规则库的 RewriteSubquery|
|RewriteSubquery |RemoveRedundantProject|删除无效查询|同 Aggregate 规则库的RemoveRedundantProject,无效查询是指父查询和子查询目标字段相同|




