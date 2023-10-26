/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.parser.ddl

import org.bitlap.core.sql.parser.BitlapSqlDdlNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{ SqlIdentifier, SqlKind, SqlNode, SqlSpecialOperator, SqlWriter }
import org.apache.calcite.sql.parser.SqlParserPos

/** Parse tree for `SHOW (DATABASES | SCHEMAS)` statement.
 */
class SqlShowDatabases(override val _pos: SqlParserPos, val pattern: SqlIdentifier)
    extends BitlapSqlDdlNode(_pos, SqlShowDatabases.OPERATOR, List.empty[SqlNode]) {

  override def unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int): Unit = {
    writer.keyword("SHOW DATABASES")
  }

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "database_name" -> SqlTypeName.VARCHAR
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    catalog.listDatabases().map { it => Array(it.name) }
  }
}

object SqlShowDatabases {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("SHOW DATABASES", SqlKind.OTHER)
}
