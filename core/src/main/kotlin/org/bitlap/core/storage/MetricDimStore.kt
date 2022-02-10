package org.bitlap.core.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapIterator
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta

/**
 * Metric store with one low cardinality dimension
 */
abstract class MetricDimStore(table: Table, hadoopConf: Configuration) :
    AbsBitlapStore<MetricDimRow>(Path(table.path), hadoopConf) {

    abstract fun queryMeta(
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter,
    ): BitlapIterator<MetricDimRowMeta>

    abstract fun queryBBM(
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter,
    ): BitlapIterator<MetricDimRow>

    abstract fun queryCBM(
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter,
    ): BitlapIterator<MetricDimRow>
}
