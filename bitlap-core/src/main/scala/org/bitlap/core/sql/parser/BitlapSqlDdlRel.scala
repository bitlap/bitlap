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

import org.bitlap.core.sql.table.BitlapSqlDdlTable

import org.apache.calcite.DataContext
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.tools.RelBuilder

import com.google.common.collect.ImmutableList

trait BitlapSqlDdlRel {

  /** sql operator
   */
  val op: SqlOperator

  /** ddl result type definition
   */
  val resultTypes: List[(String, SqlTypeName)]

  /** operator of this ddl node
   */
  def operator(context: DataContext): List[Array[Any]]

  /** get rel plan from this node
   */
  def rel(relBuilder: RelBuilder): RelNode = {
    val table = BitlapSqlDdlTable(this.resultTypes, this.operator)
    LogicalTableScan.create(
      relBuilder.getCluster,
      RelOptTableImpl.create(
        null,
        table.getRowType(relBuilder.getTypeFactory),
        table,
        ImmutableList.of()
      ),
      ImmutableList.of()
    )
  }
}
