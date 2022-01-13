package org.bitlap.core.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapIterator
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta

/**
 * Metric store with one low cardinality dimension
 */
abstract class MetricDimStore(table: Table, hadoopConf: Configuration) :
    AbsBitlapStore<MetricDimRow>(Path(table.path), hadoopConf) {

    abstract fun queryMeta(
        timeFilter: TimeFilterFun,
        metrics: List<String>,
        dimension: Pair<String, List<String>>
    ): BitlapIterator<MetricDimRowMeta>
}
