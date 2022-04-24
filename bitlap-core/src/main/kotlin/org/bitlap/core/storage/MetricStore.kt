/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapIterator
import org.bitlap.common.TimeRange
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.PruneTimeFilter
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta

/**
 * Metric store
 */
abstract class MetricStore(table: Table, hadoopConf: Configuration) :
    AbsBitlapStore<MetricRow>(Path(table.path), hadoopConf) {

    abstract fun query(time: TimeRange, metrics: List<String>): BitlapIterator<MetricRow>

    abstract fun queryMeta(time: TimeRange, metrics: List<String>): BitlapIterator<MetricRowMeta>

    abstract fun queryMeta(timeFilter: PruneTimeFilter, metrics: List<String>): BitlapIterator<MetricRowMeta>

    abstract fun queryBBM(timeFilter: PruneTimeFilter, metrics: List<String>): BitlapIterator<MetricRow>

    abstract fun queryCBM(timeFilter: PruneTimeFilter, metrics: List<String>): BitlapIterator<MetricRow>
}