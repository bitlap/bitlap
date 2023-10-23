/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.parser.ddl

import org.bitlap.common.utils.StringEx
import org.bitlap.core.sql.QueryContext
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos

/** Desc: Parse tree for `SHOW (TABLES | DATASOURCES) [IN schema_name]` statement.
 *
 *  Mail: chk19940609@gmail.com Created by IceMimosa Date: 2021/8/25
 */
class SqlShowTables(
  override val _pos: SqlParserPos,
  val database: SqlIdentifier)
    extends BitlapSqlDdlNode(_pos, SqlShowTables.OPERATOR, List(database).filter(_ != null)) {

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "database_name"    -> SqlTypeName.VARCHAR,
    "table_name"       -> SqlTypeName.VARCHAR,
    "create_timestamp" -> SqlTypeName.TIMESTAMP
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    val currentSchema = StringEx.blankOr(
      Option(database).map(_.getSimple).getOrElse(""),
      QueryContext.get().currentSchema
    )
    return catalog.listTables(currentSchema).map { it =>
      Array(it.database, it.name, it.createTime)
    }
  }
}

object SqlShowTables {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("SHOW TABLES", SqlKind.OTHER)
}
