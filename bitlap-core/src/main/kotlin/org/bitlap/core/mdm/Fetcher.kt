/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm

import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

/**
 * Fetch core data from local storage system or remote.
 */
interface Fetcher {

    fun fetchMetrics(
        table: Table,
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        metricType: Class<out DataType>,
    ): RowIterator

    fun fetchMetrics(
        table: Table,
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        metricType: Class<out DataType>,
        dimension: String,
        dimensionFilter: PrunePushedFilter,
    ): RowIterator
}
