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

import org.bitlap.core.BitlapContext
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos

/** explain a sql query
 */
class SqlExplainX(
  override val _pos: SqlParserPos,
  private val stmt: SqlNode)
    extends BitlapSqlDdlNode(_pos, SqlExplainX.OPERATOR, List(stmt)) {

  override val resultTypes: List[(String, SqlTypeName)] = List(
    "result" -> SqlTypeName.VARCHAR
  )

  override def operator(context: DataContext): List[Array[Any]] = {
    stmt match {
      case _: SqlSelect => {
        val plan = BitlapContext.sqlPlanner.parse(stmt.toString)
        List(Array(plan.explain()))
      }
      case _ => {
        throw IllegalArgumentException(s"Illegal explain SqlSelect node $stmt")
      }
    }
  }
}

object SqlExplainX {
  val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("EXPLAIN", SqlKind.OTHER)
}
