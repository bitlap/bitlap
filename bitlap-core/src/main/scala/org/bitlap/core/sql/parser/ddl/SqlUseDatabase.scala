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

import org.bitlap.common.exception.BitlapException
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos

/** Parse tree for `USE database` statement.
 */
class SqlUseDatabase(
  override val _pos: SqlParserPos,
  val database: SqlIdentifier)
    extends BitlapSqlDdlNode(_pos, SqlUseDatabase.OPERATOR, List(database).filter(_ != null)) {

  var useDatabase: String = _

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "result" -> SqlTypeName.VARCHAR
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    if (database == null || database.getSimple == null) {
      throw BitlapException(s"Unable to use database with null.")
    }
    val db = catalog.getDatabase(database.getSimple)
    this.useDatabase = db.name
    List(Array(db.name))
  }
}

object SqlUseDatabase {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("USE", SqlKind.OTHER)
}
