/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql

import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.avatica.util.Quoting
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
import org.apache.calcite.prepare.PlannerImpl
import org.apache.calcite.rel.RelCollationTraitDef
import org.apache.calcite.rel.RelDistributionTraitDef
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.runtime.Hook
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.bitlap.BitlapSqlParserImpl
import org.apache.calcite.sql.util.ListSqlOperatorTable
import org.apache.calcite.sql.util.SqlOperatorTables
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools.FrameworkConfig
import org.apache.calcite.tools.Frameworks
import org.apache.calcite.tools.RelBuilder
import org.bitlap.core.Constants.DEFAULT_DATABASE
import org.bitlap.core.data.BitlapCatalog
import org.bitlap.core.sql.parser.BitlapSqlDdlRel
import org.bitlap.core.sql.rule.ENUMERABLE_RULES
import org.bitlap.core.sql.rule.RULES
import org.bitlap.core.sql.table.BitlapSqlQueryTable
import org.bitlap.core.sql.udf.FunctionRegistry

/**
 * Desc: link [PlannerImpl]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/13
 */
class BitlapSqlPlanner(private val catalog: BitlapCatalog) {

    /**
     * Parse a [statement] to a [RelNode]
     */
    fun parse(statement: String): BitlapSqlPlan {
        System.setProperty("calcite.default.charset", "utf8")
        // get schemas from catalog
        val schema = this.buildSchemas()
        // register user-defined functions
        val listSqlOperatorTable = ListSqlOperatorTable().apply {
            FunctionRegistry.sqlFunctions().forEach { add(it) }
        }
        val config = Frameworks.newConfigBuilder()
            .parserConfig(
                SqlParser.config()
                    .withParserFactory(BitlapSqlParserImpl.FACTORY)
                    .withCaseSensitive(false)
                    .withQuoting(Quoting.BACK_TICK)
                    .withQuotedCasing(Casing.TO_LOWER)
                    .withUnquotedCasing(Casing.TO_LOWER)
            )
            .defaultSchema(schema)
            .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
            .operatorTable(SqlOperatorTables.chain(listSqlOperatorTable, SqlStdOperatorTable.instance()))
            .build()

        // parser sql to sql node
        val sqlNode = QueryContext.get().let { ctx ->
            val parser = SqlParser.create(statement, config.parserConfig)
            parser.parseQuery().also {
                ctx.originalPlan = it
            }
        }
        return when (sqlNode) {
            is BitlapSqlDdlRel -> {
                val rel = sqlNode.rel(RelBuilder.create(config))
                BitlapSqlPlan(statement, rel, rel)
            }
            else -> {
                // 1. validate sql plan
                val validator = this.buildValidator(config)
                val sqlNodeV = validator.validate(sqlNode)

                // 2. parse sql node to logical plan
                val planner = VolcanoPlanner(config.costFactory, config.context).also { p ->
                    RelOptUtil.registerDefaultRules(
                        p,
                        validator.connConfig.materializationsEnabled(),
                        Hook.ENABLE_BINDABLE.get(false)
                    )
                    p.executor = config.executor
                    p.clearRelTraitDefs()
                    config.traitDefs?.forEach { p.addRelTraitDef(it) }
                    p.addRelTraitDef(ConventionTraitDef.INSTANCE)
                    ENUMERABLE_RULES.forEach { p.addRule(it) }
                }
                val cluster = RelOptCluster.create(planner, RexBuilder(validator.typeFactory))

                val sqlToRelConverterConfig = config.sqlToRelConverterConfig.withTrimUnusedFields(false)
                val sqlToRelConverter = SqlToRelConverter(
                    null,
                    validator,
                    validator.catalogReader,
                    cluster,
                    config.convertletTable,
                    sqlToRelConverterConfig
                )
                var root = sqlToRelConverter.convertQuery(sqlNodeV, false, true)
                root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
                val relBuilder = sqlToRelConverterConfig.relBuilderFactory.create(cluster, null)
                root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder))

                var relNode = root.rel
                relNode = RULES.fold(relNode) { rel, rules ->
                    val builder = HepProgramBuilder()
                    builder.addRuleCollection(rules)
                    val hepPlanner = HepPlanner(builder.build())
                    hepPlanner.root = rel
                    hepPlanner.findBestExp()
                }
                BitlapSqlPlan(statement, root.rel, relNode)
            }
        }
    }

    private fun buildSchemas(): SchemaPlus {
        val root = Frameworks.createRootSchema(true)
        this.catalog.listDatabases().forEach {
            val dbName = it.name
            val schema = CalciteSchema.createRootSchema(true, true, dbName, root).plus()
            catalog.listTables(dbName).forEach { t ->
                if (dbName == DEFAULT_DATABASE) {
                    root.add(t.name, BitlapSqlQueryTable(t))
                } else {
                    schema.add(t.name, BitlapSqlQueryTable(t))
                }
            }
            root.add(dbName, schema)
        }
        return root
    }

    private fun buildValidator(config: FrameworkConfig): BitlapSqlValidator {
        val parserConfig = config.parserConfig
        val sqlValidatorConfig = config.sqlValidatorConfig

        var connConfig =
            config.context.maybeUnwrap(CalciteConnectionConfigImpl::class.java).orElse(CalciteConnectionConfig.DEFAULT)
        if (!connConfig.isSet(CalciteConnectionProperty.CASE_SENSITIVE)) {
            connConfig =
                connConfig.set(CalciteConnectionProperty.CASE_SENSITIVE, parserConfig.caseSensitive().toString())
        }
        if (!connConfig.isSet(CalciteConnectionProperty.CONFORMANCE)) {
            connConfig = connConfig.set(CalciteConnectionProperty.CONFORMANCE, parserConfig.conformance().toString())
        }

        val catalogReader = CalciteCatalogReader(
            CalciteSchema.from(rootSchema(config.defaultSchema)),
            CalciteSchema.from(config.defaultSchema).path(null),
            JavaTypeFactoryImpl(config.typeSystem),
            connConfig,
        )

        return BitlapSqlValidator(
            config.operatorTable,
            catalogReader,
            sqlValidatorConfig
                .withDefaultNullCollation(connConfig.defaultNullCollation())
                .withLenientOperatorLookup(connConfig.lenientOperatorLookup())
                .withSqlConformance(connConfig.conformance())
                .withIdentifierExpansion(true),
            connConfig,
        )
    }

    private fun rootSchema(schema: SchemaPlus?): SchemaPlus? {
        var s = schema
        while (true) {
            val parentSchema = s?.parentSchema ?: return s
            s = parentSchema
        }
    }
}
