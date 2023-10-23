/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.parser.ddl

import org.bitlap.core.sql.QueryContext
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{ SqlKind, SqlNode, SqlSpecialOperator }
import org.apache.calcite.sql.parser.SqlParserPos

/** Parse tree for `SHOW current_database` statement.
 */
class SqlShowCurrentDatabase(
  override val _pos: SqlParserPos)
    extends BitlapSqlDdlNode(_pos, SqlShowCurrentDatabase.OPERATOR, List.empty[SqlNode]) {

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "database_name" -> SqlTypeName.VARCHAR
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    return List(
      Array(QueryContext.get().currentSchema) // must exist
    )
  }
}

object SqlShowCurrentDatabase {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("SHOW CURRENT_DATABASE", SqlKind.OTHER)
}
