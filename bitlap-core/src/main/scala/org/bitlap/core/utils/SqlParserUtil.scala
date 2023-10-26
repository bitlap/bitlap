/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.utils

import org.bitlap.core.catalog.BitlapCatalog
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.core.sql.table.BitlapSqlQueryTable
import org.bitlap.core.sql.udf.FunctionRegistry

import org.apache.calcite.avatica.util.{ Casing, Quoting }
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.rel.{ RelCollationTraitDef, RelDistributionTraitDef }
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.bitlap.BitlapSqlParserImpl
import org.apache.calcite.sql.util.SqlOperatorTables
import org.apache.calcite.tools.{ FrameworkConfig, Frameworks }

object SqlParserUtil {

  private val parserConfig =
    SqlParser
      .config()
      .withParserFactory(BitlapSqlParserImpl.FACTORY)
      .withCaseSensitive(false)
      .withQuoting(Quoting.BACK_TICK)
      .withQuotedCasing(Casing.TO_LOWER)
      .withUnquotedCasing(Casing.TO_LOWER)

  private def buildSchemas(catalog: BitlapCatalog): SchemaPlus = {
    val root = Frameworks.createRootSchema(true)
    catalog.listDatabases().foreach { it =>
      val dbName = it.name
      val schema = CalciteSchema.createRootSchema(true, true, dbName, root).plus()
      catalog.listTables(dbName).foreach { t =>
        if (dbName == Database.DEFAULT_DATABASE) {
          root.add(t.name, BitlapSqlQueryTable(t))
        } else {
          schema.add(t.name, BitlapSqlQueryTable(t))
        }
      }
      root.add(dbName, schema)
    }
    root
  }

  def frameworksConfig(catalog: BitlapCatalog): FrameworkConfig = {
    // get schemas from catalog
    val schema = this.buildSchemas(catalog)
    // register user-defined functions
    val listSqlOperatorTable = SqlOperatorTables.of(FunctionRegistry.sqlFunctions())
    // val listSqlOperatorTable = ListSqlOperatorTable()
    // FunctionRegistry.sqlFunctions().forEach { f => listSqlOperatorTable.add(f) }
    Frameworks
      .newConfigBuilder()
      .parserConfig(parserConfig)
      .defaultSchema(schema)
      .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
      .operatorTable(SqlOperatorTables.chain(listSqlOperatorTable, SqlStdOperatorTable.instance()))
      .build()
  }
}
