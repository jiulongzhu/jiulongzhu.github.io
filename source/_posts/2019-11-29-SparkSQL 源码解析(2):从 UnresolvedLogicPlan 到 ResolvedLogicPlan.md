---
layout:     post
title:     SparkSQL源码解析(2):从 UnsolvedLogicPlan 到 ResolvedLogicPlan
subtitle:   SparkSQL 源码解析
date:       2019-11-29
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - Spark 2.3.0  
    - Spark SQL  
    - 源码解析  
---

## 概述
Parser 模块负责将原始 SQL 文本解析成 UnResolvedLogicPlan，Anayzer 模块负责将 UnResolvedLogicPlan(Relation|Attribute|Function) 通过 SessionCatalog 信息绑定到真正可操作的实体上(File|Column|MainClass)，即 SolvedLogicPlan，并校验其是否合法。   

<!-- more -->

则关键点在于:   

1. SessionCatalog 存在目的        
2. 如何绑定，将 UnresolvedLogicPlan 转换为 ResolvedLogicPlan    
3. 如何校验 ResolvedLogicPlan 合法  

1.Analyzer 对 UnresolvedLogicPlan应用各类规则(Rule),并对各个 UnresolvedLogicPlan节点绑定 CataLog 信息,将 UnresolvedLogicPlan中的UnresolvedRelation 和 UnresolvedAttribute解析成有类型(Typed)的对象,生成解析后的逻辑算子树 SolvedLogicPlan.  
2.对 UnresolvedLogicPlan 的大部分操作,如绑定解析优化等,主要方法都是基于规则(Rule)的,然后通过模式匹配对UnresolvedLogicPlan进行相应处理.  
3.Catalog信息在Analyzer的构造函数中.  

## 源码解析  

调用栈  
->SparkSession#sql  
　->DataSet#ofRows  
　　->QueryExecution#assertAnalyzed   
　　　->QueryExecution#analyzed (lazy)  
　　　　->Analyzer#executeAndCheck  
[Anayzer.scala]

```
def executeAndCheck(plan: LogicalPlan): LogicalPlan = {
    // 第一步: 绑定
    val analyzed = execute(plan)
    try {
      // 第二步: 校验
      checkAnalysis(analyzed)
      // 将 LogicPlan 的 AnalysisBarrier 装箱去掉(如果存在)
      EliminateBarriers(analyzed)
    } catch {
      case e: AnalysisException =>
        val ae = new AnalysisException(e.message, e.line, e.startPosition, Option(analyzed))
        ae.setStackTrace(e.getStackTrace)
        throw ae
    }
  }
```
### SessionCatalog

SessionCatalog 是 SparkSession 索引维护的信息库，SessionCatalog 一方面可以管理 SparkSession 创建的临时视图和 UDF，一方面可以作为外部元数据库的代理以便 SparkSession 获取外部源数据库信息(e.g. HiveSessionCatalog 操作 Hive metastore)。    

|成员变量 | 数据类型|功能 | Catalog 类型|
|:------------- |:---------------|:---------------|:---------------|
|tableRelationCache|Cache[QualifiedTableName, LogicalPlan]|表全限定名与逻辑计划的映射|SessionCatalog|
|tempViews|HashMap[String, LogicalPlan]|临时视图名与逻辑计划的映射|SessionCatalog|
|globalTempViewManager|GlobalTempViewManager|全局视图管理|SessionCatalog|
|externalCatalog|ExternalCatalog|外部元数据库的代理,从获得连接到库表分区函数等信息获取接口|SessionCatalog|
|functionRegistry|FunctionRegistry|函数(builtin,UDF)操作代理|SessionCatalog|
|parser|ParserInterface|SQL 文本解析 Parser |SessionCatalog |
|functionResourceLoader|FunctionResourceLoader|加载函数主类资源加载器|SessionCatalog|
|metastoreCatalog|HiveMetastoreCatalog|HiveExternalCatalog 的旧接口,deprecated|HiveSessionCatalog|

引用一个通过表名绑定逻辑计划的函数: 通过表所属数据库名在全局视图，外部元数据库和临时视图内搜索以绑定逻辑计划  
[SessionCatalog.scala]

```
def lookupRelation(name: TableIdentifier): LogicalPlan = {
    synchronized {
      val db = formatDatabaseName(name.database.getOrElse(currentDb))
      val table = formatTableName(name.table)
      // 全局视图
      if (db == globalTempViewManager.database) {
        globalTempViewManager.get(table).map { viewDef =>
          SubqueryAlias(table, viewDef)
        }.getOrElse(throw new NoSuchTableException(db, table))
      } else if (name.database.isDefined || !tempViews.contains(table)) {
      // 外部元数据库
        val metadata = externalCatalog.getTable(db, table)
        if (metadata.tableType == CatalogTableType.VIEW) {
          val viewText = metadata.viewText.getOrElse(sys.error("Invalid view without text."))
          val child = View(
            desc = metadata,
            output = metadata.schema.toAttributes,
            child = parser.parsePlan(viewText))
          SubqueryAlias(table, child)
        } else {
          SubqueryAlias(table, UnresolvedCatalogRelation(metadata))
        }
      } else {
      // 临时视图
        SubqueryAlias(table, tempViews(table))
      }
    }
  }
```

### 绑定

Anayzer 模块对 UnResolvedLogicPlan 应用多批规则库，其中主要是 Resolution(解析)规则库和 Substitution(转换)规则库，直到每批规则库达到了指定的迭代次数或者规则库无法再优化逻辑计划为止。  

调用栈  
Analyzer#executeAndCheck  
　->Analyzer#execute  
　　->Analyzer#executeSameContext  
　　　->RuleExecutor#execute  
[RuleExecutor.scala]

```
  /**
   * 规则批次间和批次内都是串行执行
   * 逻辑计划在此处被 Analyzed 之后就替换了(result/curPlan和返回值)
   */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan
    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan	//tempPlan
      var continue = true
      // foldLeft 依次执行规则 batch 中的所有规则,直到达到了规则的最大执行次数或者逻辑算子树不再变化
      while (continue) {
        curPlan = batch.rules.foldLeft(curPlan) { 
          case (plan, rule) =>
          // 在逻辑计划上应用规则处理
            val result = rule(plan) //rule.apply(plan)
            if (!result.fastEquals(plan)) {
              logTrace(
                s"""
                  |=== Applying Rule ${rule.ruleName} ===
                  |${sideBySide(plan.treeString, result.treeString).mkString("\n")}
                """.stripMargin)
            }
            queryExecutionMetrics.incExecutionTimeBy(rule.ruleName, runTime)
            queryExecutionMetrics.incNumExecution(rule.ruleName)
            ....
             result
        }
        iteration += 1
         // 达到了该规则批策略的最大迭代次数
        if (iteration > batch.strategy.maxIterations) {
          ...
          continue = false
        }
        // 这批规则已经不能再优化逻辑计划了
        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }
      ...
    }
    curPlan
  }
```
重点是其中的 rule(plan)方法，即 Rule#apply(plan)。各规则子类依据自身的功能解析转换 TreeNode，包括绑定 UnResolvedLogicPlan 为 ResolvedLogicPlan   
 [Rule.scala]
  
```
abstract class Rule[TreeType <: TreeNode[_]] extends Logging {
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }
  def apply(plan: TreeType): TreeType
}
```
规则的具体功能列表参考"附录：规则功能列表"   

### 校验

调用栈  
Analyze#executeAndCheck  
　->CheckAndAnalysis#checkAnalysis  

检查 ResolvedLogicPlan 中不合乎语法规范的错误，使用后序遍历(先子节点后当前节点)的方式尽可能抛出最先导致失败的错误      
[CheckAndAnaysis.scala]  

```
def checkAnalysis(plan: LogicalPlan): Unit = {
    plan.foreachUp {
      case u: UnresolvedRelation =>
        u.failAnalysis(s"Table or view not found: ${u.tableIdentifier}")

      case operator: LogicalPlan =>
        operator transformExpressionsUp {
          case a: Attribute if !a.resolved =>
            val from = operator.inputSet.map(_.qualifiedName).mkString(", ")
            a.failAnalysis(s"cannot resolve '${a.sql}' given input columns: [$from]")

          case e: Expression if e.checkInputDataTypes().isFailure =>
            e.checkInputDataTypes() match {
              case TypeCheckResult.TypeCheckFailure(message) =>
                e.failAnalysis(
                  s"cannot resolve '${e.sql}' due to data type mismatch: $message")
            }

          case c: Cast if !c.resolved =>
            failAnalysis(
              s"invalid cast from ${c.child.dataType.simpleString} to ${c.dataType.simpleString}")

          case g: Grouping =>
            failAnalysis("grouping() can only be used with GroupingSets/Cube/Rollup")
          case g: GroupingID =>
            failAnalysis("grouping_id() can only be used with GroupingSets/Cube/Rollup")

          case w @ WindowExpression(AggregateExpression(_, _, true, _), _) =>
            failAnalysis(s"Distinct window functions are not supported: $w")

          case w @ WindowExpression(_: OffsetWindowFunction,
            WindowSpecDefinition(_, order, frame: SpecifiedWindowFrame))
             if order.isEmpty || !frame.isOffset =>
            failAnalysis("An offset window function can only be evaluated in an ordered " +
              s"row-based window frame with a single offset: $w")

          case w @ WindowExpression(e, s) =>
            // Only allow window functions with an aggregate expression or an offset window
            // function.
            e match {
              case _: AggregateExpression | _: OffsetWindowFunction | _: AggregateWindowFunction =>
                w
              case _ =>
                failAnalysis(s"Expression '$e' not supported within a window function.")
            }

          case s: SubqueryExpression =>
            checkSubqueryExpression(operator, s)
            s
        }

        operator match {
          case etw: EventTimeWatermark =>
            etw.eventTime.dataType match {
              case s: StructType
                if s.find(_.name == "end").map(_.dataType) == Some(TimestampType) =>
              case _: TimestampType =>
              case _ =>
                failAnalysis(
                  s"Event time must be defined on a window or a timestamp, but " +
                  s"${etw.eventTime.name} is of type ${etw.eventTime.dataType.simpleString}")
            }
          case f: Filter if f.condition.dataType != BooleanType =>
            failAnalysis(
              s"filter expression '${f.condition.sql}' " +
                s"of type ${f.condition.dataType.simpleString} is not a boolean.")

          case Filter(condition, _) if hasNullAwarePredicateWithinNot(condition) =>
            failAnalysis("Null-aware predicate sub-queries cannot be used in nested " +
              s"conditions: $condition")

          case j @ Join(_, _, _, Some(condition)) if condition.dataType != BooleanType =>
            failAnalysis(
              s"join condition '${condition.sql}' " +
                s"of type ${condition.dataType.simpleString} is not a boolean.")
	..........
	// 自定义的规则检查在内置规则检查之后才能开始
    extendedCheckRules.foreach(_(plan))
    plan.foreachUp {
      case AnalysisBarrier(child) if !child.resolved => checkAnalysis(child)
      case o if !o.resolved => failAnalysis(s"unresolved operator ${o.simpleString}")
      case _ =>
    }
  }
```

## TODO

UnResolvedLogicPlan 在绑定了 Catalog 之后可以转换为 RDD 模式来执行了，但是由于提交的 SQL 质量参差不齐，按照 ResolvedLogicPlan 按部就班的执行会导致代价/效率差距很大 且 要求用户对执行引擎的执行模式很了解并熟悉 SQL 优化手段才能写出效率高执行快的 SQL。所以为了尽可能忽略用户的代码质量，对SQL 优化的熟悉程度，SparkSQL 都需要以很高的效率执行，SparkSQL 在后续阶段需要对 UnResolvedLogicPlan 进行优化，即 Rule-Based Optimizer，也称为 RBO。    

## 附录：规则功能列表

规则库 Batch: 在 UnResolvedLogicPlan 上应用规则库 rules 的执行策略 strategy。 
[Batch.scala]  

```
case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)
```
Strategy 表示规则批的最大迭代次数，有两个子类: Once 表明只需应用一次规则库即可；FixedPoint 表示最大可以应用规则库 maxIterations 次，如果应用中途无法再优化逻辑计划则跳出。   

```
abstract class Strategy { def maxIterations: Int }
case object Once extends Strategy { val maxIterations = 1 }
case class FixedPoint(maxIterations: Int) extends Strategy
```
规则 Rule 的子类有很多,在 Analyze.scala 中使用的所有规则如下  
[Analyzer.scala]

```
  lazy val batches: Seq[Batch] = Seq(
    Batch("Hints", fixedPoint,
      new ResolveHints.ResolveBroadcastHints(conf),
      ResolveHints.RemoveAllHints),
    Batch("Simple Sanity Check", Once,
      LookupFunctions),
    Batch("Substitution", fixedPoint,
      CTESubstitution,
      WindowsSubstitution,
      EliminateUnions,
      new SubstituteUnresolvedOrdinals(conf)),
    Batch("Resolution", fixedPoint,
      ResolveTableValuedFunctions ::
      ResolveRelations ::
      ResolveReferences ::
      ResolveCreateNamedStruct ::
      ResolveDeserializer ::
      ResolveNewInstance ::
      ResolveUpCast ::
      ResolveGroupingAnalytics ::
      ResolvePivot ::
      ResolveOrdinalInOrderByAndGroupBy ::
      ResolveAggAliasInGroupBy ::
      ResolveMissingReferences ::
      ExtractGenerator ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ResolveSubquery ::
      ResolveSubqueryColumnAliases ::
      ResolveWindowOrder ::
      ResolveWindowFrame ::
      ResolveNaturalAndUsingJoin ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      TimeWindowing ::
      ResolveInlineTables(conf) ::
      ResolveTimeZone(conf) ::
      TypeCoercion.typeCoercionRules(conf) ++
      extendedResolutionRules : _*),
    Batch("Post-Hoc Resolution", Once, postHocResolutionRules: _*),
    Batch("View", Once,
      AliasViewChild(conf)),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("UDF", Once,
      HandleNullInputsForUDF),
    Batch("FixNullability", Once,
      FixNullability),
    Batch("Subquery", Once,
      UpdateOuterReferences),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )
```

其中最重要的规则库是Substitution(替换)和Resolution(解析绑定)。这些规则库之间及规则库之内都是有序的，使用规则时也是串行执行的。打乱了顺序可能有转换不充分等未知的错误  
下表主要参照 org.apache.spark.sql.catalyst.parser.SqlBase.g4 文件和源码，带问号的为不确定的。

|  规则批次   |规则  | 功能| 备注|
|  ----  | ----  |----  |----  |
| Hints |  ResolveBroadcastHints|广播|mapjoin|
| Hints | RemoveAllHints |删除无效标识符||
|Simple Sanity Check| LookupFunctions|函数存在性检验||
| Substitution |CTESubstitution|with,合并计划|with a as something select * from a |
| Substitution |WindowsSubstitution|窗口函数|row_number() over (partitioned by a sort by b desc) |
| Substitution |EliminateUnions|union的计划只有一个时删除 union||
| Resolution |ResolveTableValuedFunctions|解析可作为表的函数|range函数|
| Resolution |ResolveRelations|表绑定catalog中的逻辑计划|对应createXXXView接口|
| Resolution |ResolveReferences|展开星号绑定列|select a.* from|
| Resolution |ResolveCreateNamedStruct|解析结构体构造方法|?,没见过hql中使用结构体|
| Resolution |ResolveDeserializer|解析反序列化类|decoder|
| Resolution |ResolveNewInstance|解析创建实例|encoder|
| Resolution |ResolveUpCast|解析类型转换|cast,在丢失精度时抛异常|
| Resolution |ResolveGroupingAnalytics|解析rollup多维度分析|group by a,b,c with rollup 等价于 group by a,b,c grouping sets((a,b,c),(a,b),(a),()) |
| Resolution |ResolvePivot|行转列|pivot 接口|
| Resolution |ResolveOrdinalInOrderByAndGroupBy|解析order/sort/group by语句的下标数字||
| Resolution |ResolveAggAliasInGroupBy|解析聚合时的表达式|group by case when then x else y end|
| Resolution |ResolveMissingReferences|解析在排序时不存在的列,加上但隐藏该列|select a from ... order by b|
| Resolution |ExtractGenerator|解析UDTF生成器|select explode(xx) from ..|
| Resolution |ResolveGenerate|?|?|
| Resolution |ResolveFunctions|解析函数为表达式||
| Resolution |ResolveAliases|解析生成别名表达式|?|
| Resolution |ResolveSubquery|解析子查询||
| Resolution |ResolveSubqueryColumnAliases|解析子查询列别名||
| Resolution |ResolveWindowOrder|解析窗口函数中的排序| over partitioned by .. order by..|
| Resolution |ResolveWindowFrame|解析检验窗口函数||
| Resolution |ResolveNaturalAndUsingJoin|通过输出列解析自然连接||
| Resolution |ExtractWindowExpressions|提取窗口函数表达式||
| Resolution |GlobalAggregates|解析全局聚合|select max(a) from tbl|
| Resolution |ResolveAggregateFunctions|解析不在聚合中的聚合函数|having/order by |
| Resolution |TimeWindowing|解析滑动时间窗口||
| Resolution |ResolveInlineTables|解析内联表为LocalRelation| select * fom values(..),(..) as (columns)|
| Resolution |ResolveTimeZone|解析时区表达式||
| Resolution |TypeCoercion.typeCoercionRules|强制转换为兼容类型|在比较和 union 时使用,不损失精度|
| Resolution |extendedResolutionRules|拓展规则,空集合|?|
|Post-Hoc Resolution|postHocResolutionRules|Resolution 规则后执行的规则.空集合||
|View|AliasViewChild|视图的分析规则||
|Nondeterministic|PullOutNondeterministic|提取非确定性表达式,放到child 中|?|
|UDF|HandleNullInputsForUDF|对UDF增加基本数据类型null处理(空输入则空输出)||
|FixNullability|FixNullability|通过 child 字段的 Nullablity 修复父逻辑计划字段的 Nullablity||
|Subquery|UpdateOuterReferences|聚合表达式下推|?|
|Cleanup|CleanupAliases|删除不需要的别名||

### with 

with a  as ... select * from a 语法是 SQL 语句中的一个特殊 case，with 子句打乱了 SQL 解析的通用模式，无法从上至下从左至右进行解析，只能将整个语句解析为两个相对独立的逻辑算子树，然后通过别名将 with 语句的逻辑算子树加入到主体逻辑算子树的下面  

[WindowsSubstitution.scala] 

```
  object WindowsSubstitution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
      //使用多层 match case 来匹配拿到 window 函数,
      case WithWindowDefinition(windowDefinitions, child) =>
        child.transform {
          case p => p.transformExpressions {
            case UnresolvedWindowExpression(c, WindowSpecReference(windowName)) =>
              val errorMessage =
                s"Window specification $windowName is not defined in the WINDOW clause."
              val windowSpecDefinition =
                windowDefinitions.getOrElse(windowName, failAnalysis(errorMessage))
              WindowExpression(c, windowSpecDefinition)
          }
        }
    } 
```
### relation 

绑定关系  
[ResolveRelations.scala]

```
  object ResolveRelations extends Rule[LogicalPlan] {
   // 后序遍历尝试绑定每个匹配的节点到 ResolvedLogicPlan
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
      case i @ InsertIntoTable(u: UnresolvedRelation, parts, child, _, _) if child.resolved =>
       // lookupTableFromCatalog(u) 从全局视图>外接元数据库>临时视图中绑定实体  
       // EliminateSubqueryAliases 去掉别名将子查询查询计划剥离出来
        EliminateSubqueryAliases(lookupTableFromCatalog(u)) match {
          case v: View =>
            u.failAnalysis(s"Inserting into a view is not allowed. View: ${v.desc.identifier}.")
          case other => i.copy(table = other)
        }
      case u: UnresolvedRelation => resolveRelation(u)	
    }
    先看第一个 case,如果是 insert into 句法的话,借助 catalog的<tableName,LogicPlan>缓存绑定表为已解析过的逻辑计划
    private def lookupTableFromCatalog(
        u: UnresolvedRelation,
        defaultDatabase: Option[String] = None): LogicalPlan = {
      val tableIdentWithDb = u.tableIdentifier.copy(
        database = u.tableIdentifier.database.orElse(defaultDatabase))
      try {
        catalog.lookupRelation(tableIdentWithDb)
      } catch {
		....        
          }
    }
  }
 [SessionCatalog.scala] 
  def lookupRelation(name: TableIdentifier): LogicalPlan = {
    synchronized {
      val db = formatDatabaseName(name.database.getOrElse(currentDb))
      val table = formatTableName(name.table)
      //从全局临时视图,外接元数据库,临时视图中查找该表的逻辑计划
      if (db == globalTempViewManager.database) {
        globalTempViewManager.get(table).map { viewDef =>
          SubqueryAlias(table, viewDef)
        }.getOrElse(throw new NoSuchTableException(db, table))
      } else if (name.database.isDefined || !tempViews.contains(table)) {
        val metadata = externalCatalog.getTable(db, table)
        if (metadata.tableType == CatalogTableType.VIEW) {
          val viewText = metadata.viewText.getOrElse(sys.error("Invalid view without text."))
          val child = View(
            desc = metadata,
            output = metadata.schema.toAttributes,
            child = parser.parsePlan(viewText))
          SubqueryAlias(table, child)
        } else {
          SubqueryAlias(table, UnresolvedCatalogRelation(metadata))
        }
      } else {
        SubqueryAlias(table, tempViews(table))
      }
    }
  }
  再看ResolveRelations.apply方法的第二个 case u:UnresolvedRelation => resolveRelation(u),所以首先会进入下面方法的第一个 case, 即如果不是直接在文件上运行(from json.$path)的话,先从 Catalog 中绑定逻辑计划
  [ResolveRelation.scala]
    def resolveRelation(plan: LogicalPlan): LogicalPlan = plan match {
      case u: UnresolvedRelation if !isRunningDirectlyOnFiles(u.tableIdentifier) =>
        val defaultDatabase = AnalysisContext.get.defaultDatabase
        val foundRelation = lookupTableFromCatalog(u, defaultDatabase)
        resolveRelation(foundRelation)
      case view @ View(desc, _, child) if !child.resolved =>
        // Resolve all the UnresolvedRelations and Views in the child.
        val newChild = AnalysisContext.withAnalysisContext(desc.viewDefaultDatabase) {
          if (AnalysisContext.get.nestedViewDepth > conf.maxNestedViewDepth) {
            view.failAnalysis(s"The depth of view ${view.desc.identifier} exceeds the maximum " +
              s"view resolution depth (${conf.maxNestedViewDepth}). Analysis is aborted to " +
              s"avoid errors. Increase the value of ${SQLConf.MAX_NESTED_VIEW_DEPTH.key} to work " +
              "around this.")
          }
          executeSameContext(child)
        }
        view.copy(child = newChild)
      case p @ SubqueryAlias(_, view: View) =>
        val newChild = resolveRelation(view)
        p.copy(child = newChild)
      case _ => plan
    }
```

### LookupFunctions 

函数存在性校验  
[LookupFuncations.scala]

```
object LookupFunctions extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
     // 递归检测使用的函数在 catalog 中是否存在
      case f: UnresolvedFunction if !catalog.functionExists(f.name) =>
        withPosition(f) {
          throw new NoSuchFunctionException(f.name.database.getOrElse("default"), f.name.funcName)
        }
    }
  }
```

[SessionCatalog.scala]
```
def functionExists(name: FunctionIdentifier): Boolean = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    // 使用 FuncationRegistry 和外接元数据库检测函数是否存在
    functionRegistry.functionExists(name) ||
      externalCatalog.functionExists(db, name.funcName)
  }
```

### ResolveFunction  

将函数替换为具体的表达式  
[ResolveFunction.scala]

```
object ResolveFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
      case q: LogicalPlan =>
        q transformExpressions {
          case u if !u.childrenResolved => u // Skip until children are resolved.
          case u: UnresolvedAttribute if resolver(u.name, VirtualColumn.hiveGroupingIdName) =>
            withPosition(u) {
              Alias(GroupingID(Nil), VirtualColumn.hiveGroupingIdName)()
            }
          case u @ UnresolvedGenerator(name, children) =>
            withPosition(u) {
              catalog.lookupFunction(name, children) match {
                case generator: Generator => generator
                case other =>
                  failAnalysis(s"$name is expected to be a generator. However, " +
                    s"its class is ${other.getClass.getCanonicalName}, which is not a generator.")
              }
            }
          case u @ UnresolvedFunction(funcId, children, isDistinct) =>
            withPosition(u) {
              // 使用 catalog 寻找函数
              catalog.lookupFunction(funcId, children) match {
                case wf: AggregateWindowFunction =>
                  if (isDistinct) {
                    failAnalysis(s"${wf.prettyName} does not support the modifier DISTINCT")
                  } else {
                    wf
                  }
                case agg: AggregateFunction => AggregateExpression(agg, Complete, isDistinct)
                case other =>
                  if (isDistinct) {
                    failAnalysis(s"${other.prettyName} does not support the modifier DISTINCT")
                  } else {
                    other
                  }
              }
            }
        }
    }
  }
```
使用 SessionCatalog 优先从 FunctionRegistry 中寻找，然后从外接元数据库寻找并加载资源注册到 FunctionRegistry，有些像双亲委派模型，保证 builtin 的函数不被 UDF覆盖。   
[SessionCatalog.scala]  
  
```
def lookupFunction(
      name: FunctionIdentifier,
      children: Seq[Expression]): Expression = synchronized {
    if (name.database.isEmpty && functionRegistry.functionExists(name)) {
      // This function has been already loaded into the function registry.
      return functionRegistry.lookupFunction(name, children)
    }

    // If the name itself is not qualified, add the current database to it.
    val database = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val qualifiedName = name.copy(database = Some(database))

    if (functionRegistry.functionExists(qualifiedName)) {
      return functionRegistry.lookupFunction(qualifiedName, children)
    }
    val catalogFunction = try {
      externalCatalog.getFunction(database, name.funcName)
    } catch {
      case _: AnalysisException => failFunctionLookup(name)
      case _: NoSuchPermanentFunctionException => failFunctionLookup(name)
    }
    loadFunctionResources(catalogFunction.resources)
    registerFunction(catalogFunction.copy(identifier = qualifiedName), overrideIfExists = false)
    // Now, we need to create the Expression.
    functionRegistry.lookupFunction(qualifiedName, children)
  }
```
