/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.storage

import org.apache.hadoop.fs.Path
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta

/**
 * get table format implementation
 */
interface TableFormatProvider {
    fun getMetricWriter(output: Path): BitlapWriter<MetricRow>
    fun getMetricDimWriter(output: Path): BitlapWriter<MetricDimRow>

    fun getMetricMetaReader(
        dataPath: Path,
        timeFunc: TimeFilterFun,
        metrics: List<String>,
        projections: List<String>
    ): BitlapReader<MetricRowMeta>

    fun getMetricReader(
        dataPath: Path,
        timeFunc: TimeFilterFun,
        metrics: List<String>,
        projections: List<String>
    ): BitlapReader<MetricRow>

    fun getMetricDimMetaReader(
        dataPath: Path,
        timeFunc: TimeFilterFun,
        metrics: List<String>,
        projections: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter
    ): BitlapReader<MetricDimRowMeta>

    fun getMetricDimReader(
        dataPath: Path,
        timeFunc: TimeFilterFun,
        metrics: List<String>,
        projections: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter
    ): BitlapReader<MetricDimRow>
}
