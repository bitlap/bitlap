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

import org.bitlap.core.sql.parser.BitlapSqlDdlNode
import org.bitlap.core.sql.parser.ddl.SqlShowCurrentDatabase

import org.apache.calcite.DataContext
import org.apache.calcite.sql.*
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.parser.SqlParserPos

/** Parse tree for `AUTH username password` statement.
 */
class SqlAuth(
  override val _pos: SqlParserPos,
  val user: SqlIdentifier,
  val password: String)
    extends BitlapSqlDdlNode(
      _pos,
      SqlShowCurrentDatabase.OPERATOR,
      List(
        user,
        if (password == null) SqlLiteral.createCharString("", _pos) else SqlLiteral.createCharString(password, _pos)
      )
    ) {

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "result" -> SqlTypeName.BOOLEAN
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    val splits = user.names
    val result = if (password == null) {
      catalog.auth(username = splits.get(0), "")
    } else {
      catalog.auth(username = splits.get(0), password = password)
    }
    List(Array(result))
  }
}

object SqlAuth {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("AUTH", SqlKind.OTHER)
}
