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

import org.bitlap.common.LiteralSQL._
import org.bitlap.common.utils.StringEx.*
import org.bitlap.core.sql.QueryContext
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.*
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.parser.SqlParserPos

/** Parse tree for `SHOW USERS` statement.
 */
class SqlShowUsers(
  override val _pos: SqlParserPos)
    extends BitlapSqlDdlNode(_pos, SqlShowUsers.OPERATOR, List.empty) {

  override def unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int): Unit = {
    writer.keyword(ShowUsers.command)
  }

  override val resultTypes: List[(String, SqlTypeName)] = List(
    // TODO access time, create time
    "user_name" -> SqlTypeName.VARCHAR
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    catalog.listUsers.map { it => Array(it.name) }
  }
}

object SqlShowUsers {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator(ShowUsers.command, SqlKind.OTHER)
}
