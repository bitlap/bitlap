/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.parser

import arrow.core.curried
import com.google.common.collect.ImmutableList
import org.apache.calcite.DataContext
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.tools.RelBuilder
import org.bitlap.core.SessionId
import org.bitlap.core.sql.table.BitlapSqlDdlTable

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/28
 */
interface BitlapSqlDdlRel {

    /**
     * ddl result type definition
     */
    val resultTypes: List<Pair<String, SqlTypeName>>

    /**
     * operator of this ddl node
     */
    fun operator(sessionId: SessionId, context: DataContext): List<Array<Any?>>

    /**
     * get rel plan from this node
     */
    fun rel(relBuilder: RelBuilder, sessionId: SessionId): RelNode {
        val op = this::operator.curried().invoke(sessionId)
        val table = BitlapSqlDdlTable(this.resultTypes, op)
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
}
