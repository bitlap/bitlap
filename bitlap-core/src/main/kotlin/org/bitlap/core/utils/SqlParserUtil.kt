/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.utils

import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.avatica.util.Quoting
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.rel.RelCollationTraitDef
import org.apache.calcite.rel.RelDistributionTraitDef
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.bitlap.BitlapSqlParserImpl
import org.apache.calcite.sql.util.ListSqlOperatorTable
import org.apache.calcite.sql.util.SqlOperatorTables
import org.apache.calcite.tools.FrameworkConfig
import org.apache.calcite.tools.Frameworks
import org.bitlap.core.catalog.BitlapCatalog
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.core.sql.table.BitlapSqlQueryTable
import org.bitlap.core.sql.udf.FunctionRegistry

/**
 *
 * @author 梦境迷离
 * @version 1.0,2022/11/11
 */
object SqlParserUtil {

    private val parserConfig =
        SqlParser.config().withParserFactory(BitlapSqlParserImpl.FACTORY).withCaseSensitive(false)
            .withQuoting(Quoting.BACK_TICK).withQuotedCasing(Casing.TO_LOWER).withUnquotedCasing(Casing.TO_LOWER)

    private fun buildSchemas(catalog: BitlapCatalog): SchemaPlus {
        val root = Frameworks.createRootSchema(true)
        catalog.listDatabases().forEach {
            val dbName = it.name
            val schema = CalciteSchema.createRootSchema(true, true, dbName, root).plus()
            catalog.listTables(dbName).forEach { t ->
                if (dbName == Database.DEFAULT_DATABASE) {
                    root.add(t.name, BitlapSqlQueryTable(t))
                } else {
                    schema.add(t.name, BitlapSqlQueryTable(t))
                }
            }
            root.add(dbName, schema)
        }
        return root
    }

    fun frameworksConfig(catalog: BitlapCatalog): FrameworkConfig {
        // get schemas from catalog
        val schema = this.buildSchemas(catalog)
        // register user-defined functions
        val listSqlOperatorTable = ListSqlOperatorTable().apply {
            FunctionRegistry.sqlFunctions().forEach { add(it) }
        }
        val config: FrameworkConfig = Frameworks.newConfigBuilder().parserConfig(parserConfig).defaultSchema(schema)
            .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
            .operatorTable(SqlOperatorTables.chain(listSqlOperatorTable, SqlStdOperatorTable.instance())).build()
        return config
    }
}
