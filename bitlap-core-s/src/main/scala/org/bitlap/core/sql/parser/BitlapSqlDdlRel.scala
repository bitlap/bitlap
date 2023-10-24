/**
 * Copyright (C) 2023 bitlap.org .
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

/** Mail: chk19940609@gmail.com Created by IceMimosa Date: 2021/7/28
 */
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
