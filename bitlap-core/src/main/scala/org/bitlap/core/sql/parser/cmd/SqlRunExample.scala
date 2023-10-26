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
package org.bitlap.core.sql.parser.cmd

import org.bitlap.core.sql.parser.BitlapSqlDdlNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos

/** run example, this is an example, will be removed in future versions.
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
