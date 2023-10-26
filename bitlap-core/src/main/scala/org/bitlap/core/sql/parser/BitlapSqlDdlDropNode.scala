/**
 * Copyright (C) 2023 bitlap.org .
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
