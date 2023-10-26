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
    List(
      Array(QueryContext.get().currentSchema) // must exist
    )
  }
}

object SqlShowCurrentDatabase {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("SHOW CURRENT_DATABASE", SqlKind.OTHER)
}
