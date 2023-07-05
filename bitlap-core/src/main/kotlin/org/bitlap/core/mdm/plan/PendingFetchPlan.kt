/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm.plan

import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.MDColumnAnalyzer
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

/**
 * Desc: Pending fetch plan to be analyzed
 */
class PendingFetchPlan(
    val table: Table,
    val analyzer: MDColumnAnalyzer,
    val timeFilter: PruneTimeFilter,
    val pushedFilter: PrunePushedFilter,
) : AbsFetchPlan() {
    override fun execute(context: FetchContext): RowIterator {
        throw IllegalStateException("Bitlap PendingFetchPlan cannot be executed")
    }

    override fun explain(depth: Int): String {
        return "${" ".repeat(depth)}+- PendingFetchPlan"
    }
}
