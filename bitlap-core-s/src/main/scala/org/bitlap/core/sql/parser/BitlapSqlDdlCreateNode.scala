/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.parser

import java.util.List as JList

import scala.jdk.CollectionConverters._

import org.bitlap.core.BitlapContext

import org.apache.calcite.sql.SqlCreate
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.parser.SqlParserPos

/** Mail: chk19940609@gmail.com Created by IceMimosa Date: 2021/7/28
 */
abstract class BitlapSqlDdlCreateNode(
  val _pos: SqlParserPos,
  override val op: SqlOperator,
  val operands: List[SqlNode],
  val _replace: Boolean,
  val _ifNotExists: Boolean)
    extends SqlCreate(op, _pos, _replace, _ifNotExists)
    with BitlapSqlDdlRel {

  protected val catalog = BitlapContext.catalog

  override def getOperator(): SqlOperator       = this.op
  override def getOperandList(): JList[SqlNode] = this.operands.asJava
}
