/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.core.sql.parser.ddl

import org.bitlap.core.sql.parser.BitlapSqlDdlCreateNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos

/** Parse tree for `CREATE (TABLE | DATASOURCE) IF NOT EXISTS table_name` statement.
 */
class SqlCreateTable(
  override val _pos: SqlParserPos,
  val name: SqlIdentifier,
  override val _ifNotExists: Boolean,
  override val _replace: Boolean = false)
    extends BitlapSqlDdlCreateNode(
      _pos,
      SqlCreateTable.OPERATOR,
      List(SqlLiteral.createBoolean(_replace, _pos), SqlLiteral.createBoolean(_ifNotExists, _pos), name),
      _replace,
      _ifNotExists
    ) {

  override def unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int): Unit = {
    writer.keyword("CREATE TABLE")
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS")
    }
    name.unparse(writer, leftPrec, rightPrec)
  }

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "result" -> SqlTypeName.BOOLEAN
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    val splits = name.names
    val result = if (splits.size == 1) {
      catalog.createTable(splits.get(0), ifNotExists = ifNotExists)
    } else {
      catalog.createTable(splits.get(1), splits.get(0), ifNotExists)
    }
    List(Array(result))
  }
}

object SqlCreateTable {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE)
}
