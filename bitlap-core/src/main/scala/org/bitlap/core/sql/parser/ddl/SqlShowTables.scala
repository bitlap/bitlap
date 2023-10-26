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

import org.bitlap.common.utils.StringEx
import org.bitlap.core.sql.QueryContext
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos

/** Parse tree for `SHOW (TABLES | DATASOURCES) [IN schema_name]` statement.
 */
class SqlShowTables(
  override val _pos: SqlParserPos,
  val database: SqlIdentifier)
    extends BitlapSqlDdlNode(_pos, SqlShowTables.OPERATOR, List(database).filter(_ != null)) {

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "database_name"    -> SqlTypeName.VARCHAR,
    "table_name"       -> SqlTypeName.VARCHAR,
    "create_timestamp" -> SqlTypeName.TIMESTAMP
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    val currentSchema = StringEx.blankOr(
      Option(database).map(_.getSimple).getOrElse(""),
      QueryContext.get().currentSchema
    )
    catalog.listTables(currentSchema).map { it =>
      Array(it.database, it.name, it.createTime)
    }
  }
}

object SqlShowTables {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("SHOW TABLES", SqlKind.OTHER)
}
