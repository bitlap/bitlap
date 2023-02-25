/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.parser

import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.bitlap.core.BitlapContext

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/28
 */
abstract class BitlapSqlDdlNode(
    open val pos: SqlParserPos,
    open val op: SqlOperator,
    open val operands: List<SqlNode>
) : SqlCall(pos), BitlapSqlDdlRel {

    protected val catalog = BitlapContext.catalog

    override fun getOperator(): SqlOperator = this.op
    override fun getOperandList(): List<SqlNode> = this.operands
}
