package org.bitlap.core.mdm

import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.TimeFilterFun

/**
 * Fetch core data from local storage system or remote.
 */
interface Fetcher {

    fun fetchMetrics(
        table: Table,
        timeFilter: TimeFilterFun,
        metrics: List<String>,
        metricType: Class<out DataType>,
    ): RowIterator
}
