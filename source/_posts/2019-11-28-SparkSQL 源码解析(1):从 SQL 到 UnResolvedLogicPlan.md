---
layout:     post
title:      SparkSQL源码解析(1):从 SQL 到 UnResolvedLogicPlan
subtitle:   SparkSQL 源码解析
date:       2019-11-28
author:     jiulongzhu
header-img: img/moon_night.jpg
catalog: true
tags:
    - Spark 2.3.0  
    - Spark SQL  
    - 源码解析  
---

## 概览
Parser模块 SparkSqlParser 持有的 SparkSqlAstBuilder 遍历 ANTLR 生成的词法/句法解析器解析成的语法树节点转换成相应的 LogicPlan节点，此时的 LogicPlan 节点仅仅从原始 SQL 文本中解析出来，不包含[表|列|函数]信息，因而称之为未解析的逻辑算子树 UnresolvedLogicPlan。

<!-- more -->

## 源码解析

SparkSession#sql(sqlText) 首先使用 sessionState 持有的 SparkSqlParser#parsePlan(sqlText) 将 sqlText 解析为逻辑计划，然后由 SparkSession 和逻辑计划来构建 DataFrame   
[SparkSession.scala]

```
 def sql(sqlText: String): DataFrame = {
    Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
  }
```
SparkSqlParser#parsePlan 继承自 AbstractSqlParser，SparkSqlParser 主要有两个作用: 

1. 自定义 SparkSqlAstBuilder 遍历 原始 sql 解析来的语法树每个节点 解析映射为UnResolvedLogicPlan。这是核心功能    
2.  变量替换，替换`${var}`, `${system:var}` and `${env:var}`     

[SparkSqlParser.scala]

```
class SparkSqlParser(conf: SQLConf) extends AbstractSqlParser {
  val astBuilder = new SparkSqlAstBuilder(conf)
  private val substitutor = new VariableSubstitution(conf)
  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
}
```
使用词法/语法规则解析原始 sql 为语法树的逻辑在 AbstractSqlParser#parse中，
[AbstractSqlParser.scala]

```
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
   // 第一步 : 使用子类的 parse方法来解析原始 sql 为语法树
   // 第二步: 使用子类定义的AstBuilder通过观察者模式访问语法树,转成 LogicPlan.
    astBuilder.visitSingleStatement(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ =>
        val position = Origin(None, None)
        throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
    }
    
```
第一步: 使用 ANTLR 编译 SqlBase.g4 文件定义的词法/句法来解析原始 sql 为语法树  
[AbstractSqlParser.scala]

```
  protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
   // 词法分析器
    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    // 使用 SparkSQL 的词法错误流来替换 antlr 的
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)
    val tokenStream = new CommonTokenStream(lexer)
    // 语法分析器
    val parser = new SqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    // 使用 SparkSQL 的语法错误流来替换 antlr 的
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    try {
      try {
       //SLL 和 LL 是 ANTLR对冲突和歧义的两种处理模式。SLL 速度快而功能弱,LL 相反
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case e: ParseCancellationException =>
          tokenStream.seek(0) 
          parser.reset()
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      ...
    }
  }
}
```
第二步: 使用 SparkSqlAstBuilder 来遍历语法树，生成为逻辑算子树   
调用栈   
-> SparkSqlAstBuilder.visitSingleStatement   
　->AstBuilder.visitSingleStatement   
　　->AbstractParseTreeVisitor.visit   
　　　->SingleStatementContext.accept    
　　　　->SparkSqlAstBuilder.visitChildren 
  　　　　     
引用SqlBase.g4 文件的查询规范  
[SqlBase.g4]

```
querySpecification
    : (((SELECT kind=TRANSFORM '(' namedExpressionSeq ')'
        | kind=MAP namedExpressionSeq
        | kind=REDUCE namedExpressionSeq))
       inRowFormat=rowFormat?
       (RECORDWRITER recordWriter=STRING)?
       USING script=STRING
       (AS (identifierSeq | colTypeList | ('(' (identifierSeq | colTypeList) ')')))?
       outRowFormat=rowFormat?
       (RECORDREADER recordReader=STRING)?
       fromClause?
       (WHERE where=booleanExpression)?)
    | ((kind=SELECT (hints+=hint)* setQuantifier? namedExpressionSeq fromClause?
       | fromClause (kind=SELECT setQuantifier? namedExpressionSeq)?)
       lateralView*
       (WHERE where=booleanExpression)?
       aggregation?
       (HAVING having=booleanExpression)?
       windows?)
    ;
```
引用一个最常用的查询语句的语法算子->逻辑计划算子的转换过程，最终得到的是一个逻辑计划对象       

[SparkSqlAstBuilder.scala]  

```
  override def visitQuerySpecification(
      ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
      // 第一步: 解析 from 语句，如果有多个表则转为内关联关系  
    val from = OneRowRelation.optional(ctx.fromClause) {
      visitFromClause(ctx.fromClause)
    }
    // 第二步: 基于 from 的语境来解析 select 语句关键词
    withQuerySpecification(ctx, from)
  }
 
  override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
    val from = ctx.relation.asScala.foldLeft(null: LogicalPlan) { (left, relation) =>
      val right = plan(relation.relationPrimary)
      val join = right.optionalMap(left)(Join(_, _, Inner, None))
      // 做关联
      withJoinRelations(join, relation)
    }
    ctx.lateralView.asScala.foldLeft(from)(withGenerate)
  }
  
  /**
   * Add a query specification to a logical plan. The query specification is the core of the logical
   * plan, this is where sourcing (FROM clause), transforming (SELECT TRANSFORM/MAP/REDUCE),
   * projection (SELECT), aggregation (GROUP BY ... HAVING ...) and filtering (WHERE) takes place.
   *
   * Note that query hints are ignored (both by the parser and the builder).
   */
  private def withQuerySpecification(
      ctx: QuerySpecificationContext,
      relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._

    // WHERE
    def filter(ctx: BooleanExpressionContext, plan: LogicalPlan): LogicalPlan = {
      Filter(expression(ctx), plan)
    }
    // Expressions.
    val expressions = Option(namedExpressionSeq).toSeq
      .flatMap(_.namedExpression.asScala)
      .map(typedVisit[Expression])
    // Create either a transform or a regular query.
    val specType = Option(kind).map(_.getType).getOrElse(SqlBaseParser.SELECT)
    specType match {
      case SqlBaseParser.MAP | SqlBaseParser.REDUCE | SqlBaseParser.TRANSFORM =>
        // Transform
        // Add where.
        val withFilter = relation.optionalMap(where)(filter)

        // Create the attributes.
        val (attributes, schemaLess) = if (colTypeList != null) {
          // Typed return columns.
          (createSchema(colTypeList).toAttributes, false)
        } else if (identifierSeq != null) {
          // Untyped return columns.
          val attrs = visitIdentifierSeq(identifierSeq).map { name =>
            AttributeReference(name, StringType, nullable = true)()
          }
          (attrs, false)
        } else {
          (Seq(AttributeReference("key", StringType)(),
            AttributeReference("value", StringType)()), true)
        }
        // 一元关系节点  
        ScriptTransformation(
          expressions,
          string(script),
          attributes,
          withFilter,
          withScriptIOSchema(
            ctx, inRowFormat, recordWriter, outRowFormat, recordReader, schemaLess))

      case SqlBaseParser.SELECT =>
        // Regular select
	// 解析 lateral 视图(lateral view UDTF as...),where 语句,groupBy,having,distinct 等
        // Add lateral views.
        val withLateralView = ctx.lateralView.asScala.foldLeft(relation)(withGenerate)
        // Add where.
        val withFilter = withLateralView.optionalMap(where)(filter)
        // Add aggregation or a project.
        val namedExpressions = expressions.map {
          case e: NamedExpression => e
          case e: Expression => UnresolvedAlias(e)
        }
        val withProject = if (aggregation != null) {
          withAggregation(aggregation, namedExpressions, withFilter)
        } else if (namedExpressions.nonEmpty) {
          Project(namedExpressions, withFilter)
        } else {
          withFilter
        }
        // Having
        val withHaving = withProject.optional(having) {
          // Note that we add a cast to non-predicate expressions. If the expression itself is
          // already boolean, the optimizer will get rid of the unnecessary cast.
          val predicate = expression(having) match {
            case p: Predicate => p
            case e => Cast(e, BooleanType)
          }
          Filter(predicate, withProject)
        }
        // Distinct
        val withDistinct = if (setQuantifier() != null && setQuantifier().DISTINCT() != null) {
          Distinct(withHaving)
        } else {
          withHaving
        }
        // Window
        val withWindow = withDistinct.optionalMap(windows)(withWindows)
        // Hint
        hints.asScala.foldRight(withWindow)(withHints)
    }
  }
```

## TODO

由 SparkSqlAstBuilder.scala 部分方法的返回值可知：Parser 模块的 SparkSqlParser 和 SparkSqlAstBuilder 将原始的 SQL 文本转换成了 UnResolved(Relation|Attribute|Funcation)等，但是从原始 SQL 文本解析来的Relation|Attribute|Funcation 是否存在如何解析调用均需要绑定和验证，e.g. SQL 文本中的表是 Hive Orc 表还是 JSON 文件，Funcation 代表了哪个函数 builtin 函数还是 UDF，Attribute 来自哪个基础表....  这些工作就是 Analyzer 模块的工作内容   

>
visitTable 方法的返回值是 UnresovedRelation  
visitStar 方法的返回值是 UnresolvedStar  
visitColumnReference 方法的返回值是 UnresolvedRegex 或 UnresolvedAttribute  
visitFunctionCall 方法的返回值是 UnresolvedWindowExpression 或 WindowExpression  
....





