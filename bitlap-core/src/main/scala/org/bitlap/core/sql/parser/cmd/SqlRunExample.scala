/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.parser.cmd

import org.bitlap.core.sql.parser.BitlapSqlDdlNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos

/** Desc: run example, this is an example, will be removed in future versions.
 *
 *  Mail: chk19940609@gmail.com Created by IceMimosa Date: 2021/7/28
 */
class SqlRunExample(
  override val _pos: SqlParserPos,
  private val exampleString: String)
    extends BitlapSqlDdlNode(_pos, SqlRunExample.OPERATOR, List(SqlLiteral.createCharString(exampleString, _pos))) {

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "result" -> SqlTypeName.VARCHAR
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    List(Array(s"hello $exampleString"))
  }
}

object SqlRunExample {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("RUN EXAMPLE", SqlKind.OTHER)
}
