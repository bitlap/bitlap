package org.bitlap.core.mdm

import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.model.FetchResult
import org.bitlap.core.sql.TimeFilterFun

/**
 * Fetch core data from local storage system or remote.
 */
interface Fetcher {

    fun fetchMetricsMeta(table: Table, timeFilter: TimeFilterFun, metrics: List<String>): FetchResult

//    fun fetchMetrics(table: Table, timeFilter: TimeFilterFun, metrics: List<String>): BitlapIterator<>
}
