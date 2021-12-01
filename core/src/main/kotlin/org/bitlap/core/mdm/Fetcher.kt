package org.bitlap.core.mdm

import org.bitlap.common.BitlapIterator
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.load.MetricRowMeta

/**
 * Fetch core data from local storage system or remote.
 */
interface Fetcher {

    fun fetchMetricsMeta(table: Table, timeFilter: TimeFilterFun, metrics: List<String>): BitlapIterator<MetricRowMeta>
}
