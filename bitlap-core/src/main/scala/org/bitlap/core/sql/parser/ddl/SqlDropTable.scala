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

import org.bitlap.core.sql.parser.BitlapSqlDdlDropNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos

/** Parse tree for `DROP (TABLE | DATASOURCE) IF EXISTS table_name` statement.
 */
class SqlDropTable(
  override val _pos: SqlParserPos,
  val name: SqlIdentifier,
  override val _ifExists: Boolean,
  val cascade: Boolean)
    extends BitlapSqlDdlDropNode(
      _pos,
      SqlDropTable.OPERATOR,
      List(SqlLiteral.createBoolean(_ifExists, _pos), name, SqlLiteral.createBoolean(cascade, _pos)),
      _ifExists
    ) {

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "result" -> SqlTypeName.BOOLEAN
  )

  override def unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int): Unit = {
    writer.keyword("DROP TABLE")
    if (_ifExists) {
      writer.keyword("IF EXISTS")
    }
    name.unparse(writer, leftPrec, rightPrec)
  }

  override def operator(context: DataContext): List[Array[Any]] = {
    val splits = name.names
    val result = if (splits.size == 1) {
      catalog.dropTable(splits.get(0), ifExists = ifExists, cascade = cascade)
    } else {
      catalog.dropTable(splits.get(1), splits.get(0), ifExists, cascade)
    }
    List(Array(result))
  }
}

object SqlDropTable {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE)
}
