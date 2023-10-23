/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.parser.ddl

import org.bitlap.core.sql.parser.BitlapSqlDdlDropNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos

/** Desc: Parse tree for `DROP (DATABASE | SCHEMA) IF EXISTS database_name` statement.
 *
 *  see [org.apache.calcite.sql.ddl.SqlDropSchema], you can also use [org.apache.calcite.sql.ddl.SqlDdlNodes]
 *
 *  Mail: chk19940609@gmail.com Created by IceMimosa Date: 2021/8/25
 */
class SqlDropDatabase(
  override val _pos: SqlParserPos,
  val name: SqlIdentifier,
  override val _ifExists: Boolean,
  val cascade: Boolean)
    extends BitlapSqlDdlDropNode(
      _pos,
      SqlDropDatabase.OPERATOR,
      List(SqlLiteral.createBoolean(_ifExists, _pos), name, SqlLiteral.createBoolean(cascade, _pos)),
      _ifExists
    ) {

  override def unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) = {
    writer.keyword("DROP DATABASE")
    if (_ifExists) {
      writer.keyword("IF EXISTS")
    }
    name.unparse(writer, leftPrec, rightPrec)
  }

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "result" -> SqlTypeName.BOOLEAN
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    return List(
      Array(catalog.dropDatabase(name.getSimple, ifExists, cascade))
    )
  }
}

object SqlDropDatabase {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("DROP DATABASE", SqlKind.DROP_SCHEMA)
}
