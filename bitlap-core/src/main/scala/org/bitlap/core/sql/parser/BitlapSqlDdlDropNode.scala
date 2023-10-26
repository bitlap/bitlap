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
package org.bitlap.core.sql.parser

import java.util.List as JList

import scala.jdk.CollectionConverters.*

import org.bitlap.core.BitlapContext
import org.bitlap.core.catalog.BitlapCatalog

import org.apache.calcite.sql.SqlDrop
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.parser.SqlParserPos

abstract class BitlapSqlDdlDropNode(
  val _pos: SqlParserPos,
  override val op: SqlOperator,
  val operands: List[SqlNode],
  val _ifExists: Boolean)
    extends SqlDrop(op, _pos, _ifExists)
    with BitlapSqlDdlRel {

  protected val catalog: BitlapCatalog = BitlapContext.catalog

  override def getOperator: SqlOperator       = this.op
  override def getOperandList: JList[SqlNode] = this.operands.asJava
}
