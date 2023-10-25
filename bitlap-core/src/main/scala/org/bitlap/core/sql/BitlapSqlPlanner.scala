/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import scala.jdk.CollectionConverters._

import org.bitlap.core.catalog.BitlapCatalog
import org.bitlap.core.extension._
import org.bitlap.core.sql.parser.BitlapSqlDdlRel
import org.bitlap.core.sql.rule.ENUMERABLE_RULES
import org.bitlap.core.sql.rule.RULES
import org.bitlap.core.utils.SqlParserUtil

import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.config.CalciteConnectionConfigImpl
import org.apache.calcite.config.CalciteConnectionProperty
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.hep.HepPlanner
import org.apache.calcite.plan.hep.HepProgramBuilder
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.runtime.Hook
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools.FrameworkConfig
import org.apache.calcite.tools.RelBuilder

/** Desc: link [PlannerImpl]
 *
 *  Mail: chk19940609@gmail.com Created by IceMimosa Date: 2021/9/13
 */
class BitlapSqlPlanner(private val catalog: BitlapCatalog) {

  /** Parse a [statement] to a [RelNode]
   */
  def parse(statement: String): BitlapSqlPlan = {
    System.setProperty("calcite.default.charset", "utf8")
    val config = SqlParserUtil.frameworksConfig(catalog)
    // parser sql to sql node
    val _sqlNode = QueryContext.get().let { ctx =>
      val parser = SqlParser.create(statement, config.getParserConfig)
      parser.parseQuery().also { it =>
        ctx.originalPlan = it
      }
    }
    _sqlNode match {
      case sqlNode: BitlapSqlDdlRel =>
        val rel = sqlNode.rel(RelBuilder.create(config))
        BitlapSqlPlan(statement, sqlNode, rel, rel)
      case sqlNode =>
        // 1. validate sql plan
        val validator = this.buildValidator(config)
        val sqlNodeV  = validator.validate(sqlNode)

        // 2. parse sql node to logical plan
        val planner = new VolcanoPlanner(config.getCostFactory, config.getContext).also { p =>
          RelOptUtil.registerDefaultRules(
            p,
            validator.connConfig.materializationsEnabled(),
            Hook.ENABLE_BINDABLE.get(false)
          )
          p.setExecutor(config.getExecutor)
          p.clearRelTraitDefs()
          if (config.getTraitDefs != null) {
            config.getTraitDefs.asScala.foreach(p.addRelTraitDef)
          }
          p.addRelTraitDef(ConventionTraitDef.INSTANCE)
          ENUMERABLE_RULES.foreach(p.addRule)
        }
        val cluster = RelOptCluster.create(planner, RexBuilder(validator.getTypeFactory))

        val sqlToRelConverterConfig = config.getSqlToRelConverterConfig.withTrimUnusedFields(false)
        val sqlToRelConverter = SqlToRelConverter(
          null,
          validator,
          validator._catalogReader,
          cluster,
          config.getConvertletTable,
          sqlToRelConverterConfig
        )
        var root = sqlToRelConverter.convertQuery(sqlNodeV, false, true)
        root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
        val relBuilder = sqlToRelConverterConfig.getRelBuilderFactory.create(cluster, null)
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder))

        var relNode = root.rel
        relNode = RULES.foldLeft(relNode) { case (rel, rules) =>
          val builder = HepProgramBuilder()
          builder.addRuleCollection(rules.asJava)
          val hepPlanner = HepPlanner(builder.build())
          hepPlanner.setRoot(rel)
          hepPlanner.findBestExp()
        }
        BitlapSqlPlan(statement, sqlNode, root.rel, relNode)
    }
  }

  private def buildValidator(config: FrameworkConfig): BitlapSqlValidator = {
    val parserConfig       = config.getParserConfig
    val sqlValidatorConfig = config.getSqlValidatorConfig

    var connConfig =
      config.getContext.maybeUnwrap(classOf[CalciteConnectionConfigImpl]).orElse(CalciteConnectionConfig.DEFAULT)
    if (!connConfig.isSet(CalciteConnectionProperty.CASE_SENSITIVE)) {
      connConfig = connConfig.set(CalciteConnectionProperty.CASE_SENSITIVE, parserConfig.caseSensitive().toString)
    }
    if (!connConfig.isSet(CalciteConnectionProperty.CONFORMANCE)) {
      connConfig = connConfig.set(CalciteConnectionProperty.CONFORMANCE, parserConfig.conformance().toString)
    }

    val catalogReader = CalciteCatalogReader(
      CalciteSchema.from(rootSchema(config.getDefaultSchema)),
      CalciteSchema.from(config.getDefaultSchema).path(null),
      JavaTypeFactoryImpl(config.getTypeSystem),
      connConfig
    )

    BitlapSqlValidator(
      config.getOperatorTable,
      catalogReader,
      sqlValidatorConfig
        .withDefaultNullCollation(connConfig.defaultNullCollation())
        .withLenientOperatorLookup(connConfig.lenientOperatorLookup())
        .withSqlConformance(connConfig.conformance())
        .withIdentifierExpansion(true),
      connConfig
    )
  }

  private def rootSchema(schema: SchemaPlus): SchemaPlus = {
    var s: SchemaPlus = schema
    while (true) {
      Option(s).flatMap(s => Option(s.getParentSchema)) match {
        case Some(parentSchema) =>
          s = parentSchema
        case None =>
          return s
      }
    }
    throw new IllegalStateException("This should never happen")
  }
}
