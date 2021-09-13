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
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.prepare.PlannerImpl
import org.apache.calcite.rel.RelDistributionTraitDef
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.runtime.Hook
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.ScalarFunctionImpl
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.parser.bitlap.BitlapSqlParserImpl
import org.apache.calcite.sql.type.InferTypes
import org.apache.calcite.sql.type.OperandTypes
import org.apache.calcite.sql.type.ReturnTypes
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.util.ListSqlOperatorTable
import org.apache.calcite.sql.util.SqlOperatorTables
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools.Frameworks
import org.apache.calcite.tools.RelBuilder
import org.bitlap.core.sql.parser.BitlapSqlDdlRel
import org.bitlap.core.sql.table.BitlapTable

/**
 * Desc: link [PlannerImpl]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/13
 */
class BitlapSqlPlanner {

    // TODO
    private val schema = Frameworks.createRootSchema(true).apply {
        add("test", BitlapTable())
    }

    private val listSqlOperatorTable = ListSqlOperatorTable().apply {
        add(
            SqlUserDefinedFunction(
                SqlIdentifier("hello", SqlParserPos.ZERO),
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR_2000,
                InferTypes.FIRST_KNOWN,
                OperandTypes.operandMetadata(
                    listOf(SqlTypeFamily.STRING),
                    { t -> t.builder().add("a", SqlTypeName.VARCHAR, 15).build().fieldList.map { it.type } }, { "a" }
                ) { false },
                ScalarFunctionImpl.create(Functions::class.java, "hello")
            )
        )
    }

    private val config = Frameworks.newConfigBuilder()
        .parserConfig(
            SqlParser.config()
                .withParserFactory(BitlapSqlParserImpl.FACTORY)
                .withCaseSensitive(false) // 设置大小写是否敏感
                .withQuoting(Quoting.BACK_TICK) // 设置应用标识,mysql是``
                .withQuotedCasing(Casing.TO_UPPER) // Quoting策略,不变,变大写或变小写
                .withUnquotedCasing(Casing.TO_UPPER) // 标识符没有被Quoting后的策略
        )
        .defaultSchema(schema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
        .operatorTable(SqlOperatorTables.chain(listSqlOperatorTable, SqlStdOperatorTable.instance()))
        .build()

    private val parserConfig = config.parserConfig
    private val sqlValidatorConfig = config.sqlValidatorConfig
    private val connectionConfig by lazy {
        var connConfig = config.context.maybeUnwrap(CalciteConnectionConfigImpl::class.java).orElse(CalciteConnectionConfig.DEFAULT)
        if (!connConfig.isSet(CalciteConnectionProperty.CASE_SENSITIVE)) {
            connConfig = connConfig.set(CalciteConnectionProperty.CASE_SENSITIVE, parserConfig.caseSensitive().toString())
        }
        if (!connConfig.isSet(CalciteConnectionProperty.CONFORMANCE)) {
            connConfig = connConfig.set(CalciteConnectionProperty.CONFORMANCE, parserConfig.conformance().toString())
        }
        connConfig
    }
    private val catalogReader = CalciteCatalogReader(
        CalciteSchema.from(rootSchema(config.defaultSchema)),
        CalciteSchema.from(config.defaultSchema).path(null),
        JavaTypeFactoryImpl(config.typeSystem),
        connectionConfig,
    )

    private val validator = BitlapSqlValidator(
        config.operatorTable,
        catalogReader,
        sqlValidatorConfig
            .withDefaultNullCollation(connectionConfig.defaultNullCollation())
            .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
            .withSqlConformance(connectionConfig.conformance())
            .withIdentifierExpansion(true),
    )

    companion object {
        private fun rootSchema(schema: SchemaPlus?): SchemaPlus? {
            var s = schema
            while (true) {
                val parentSchema = s?.parentSchema ?: return s
                s = parentSchema
            }
        }
    }

    fun parse(statement: String): RelNode {
        val parser = SqlParser.create(statement, config.parserConfig)
        val planner = VolcanoPlanner(config.costFactory, config.context)
        RelOptUtil.registerDefaultRules(planner, connectionConfig.materializationsEnabled(), Hook.ENABLE_BINDABLE.get(false))
        planner.executor = config.executor
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
        planner.clearRelTraitDefs()
        config.traitDefs?.forEach { planner.addRelTraitDef(it) }

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

        val sqlNode = parser.parseQuery()
        return when (sqlNode) {
            is BitlapSqlDdlRel -> {
                val bbb = RelBuilder.create(config)
                val relNode = sqlNode.rel(bbb)
                println(relNode.explain())
                relNode
            }
            else -> {
                val sqlNodeV = validator.validate(sqlNode)
                var root = sqlToRelConverter.convertQuery(sqlNodeV, false, true)
                root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
                val relBuilder = sqlToRelConverterConfig.relBuilderFactory.create(cluster, null)
                root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder))
//                val relNode = planner.rel(sqlNode)
                var relNode = root.rel
//                val desiredTraits = cluster.traitSetOf(EnumerableConvention.INSTANCE)
//                if (!relNode.getTraitSet().equals(desiredTraits)) {
//                    relNode = cluster.getPlanner().changeTraits(relNode, desiredTraits)
//                }
                println(relNode.explain())
                relNode
            }
        }
    }
}
