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
package org.bitlap.core.sql.parser.ddl.user

import org.bitlap.core.sql.parser.BitlapSqlDdlDropNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.*
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.parser.SqlParserPos

/** Parse tree for `DROP USER IF EXISTS username` statement.
 */
class SqlDropUser(
  override val _pos: SqlParserPos,
  val name: SqlIdentifier,
  override val _ifExists: Boolean)
    extends BitlapSqlDdlDropNode(
      _pos,
      SqlDropUser.OPERATOR,
      List(SqlLiteral.createBoolean(_ifExists, _pos), name),
      _ifExists
    ) {

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "result" -> SqlTypeName.BOOLEAN
  )

  override def unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int): Unit = {
    writer.keyword("DROP USER")
    if (_ifExists) {
      writer.keyword("IF EXISTS")
    }
    name.unparse(writer, leftPrec, rightPrec)
  }

  override def operator(context: DataContext): List[Array[Any]] = {
    val splits = name.names
    val result = catalog.dropUser(splits.get(0), ifExists)
    List(Array(result))
  }
}

object SqlDropUser {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("DROP USER", SqlKind.OTHER_DDL)
}
