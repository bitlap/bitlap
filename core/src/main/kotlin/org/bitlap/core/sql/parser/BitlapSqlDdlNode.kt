package org.apache.calcite.sql

import com.google.common.collect.ImmutableList
import org.apache.calcite.DataContext
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.tools.RelBuilder
import org.bitlap.core.sql.parser.BitlapSqlDdlTable

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/28
 */
abstract class BitlapSqlDdlNode(
    open val pos: SqlParserPos,
    open val op: SqlOperator,
    open val operands: List<SqlNode>
) : SqlCall(pos) {

    abstract val resultTypes: List<Pair<String, SqlTypeName>>

    abstract fun operator(context: DataContext): List<Array<Any?>>

    /**
     * get rel plan from this node
     */
    fun rel(relBuilder: RelBuilder): RelNode {
        val table = BitlapSqlDdlTable(this.resultTypes, this::operator)
        return LogicalTableScan.create(
            relBuilder.cluster,
            RelOptTableImpl.create(
                null,
                table.getRowType(relBuilder.typeFactory),
                table,
                ImmutableList.of()
            ),
            emptyList(),
        )
    }

    override fun getOperator(): SqlOperator = this.op
    override fun getOperandList(): List<SqlNode> = this.operands
}
