/**
 * Copyright (C) 2023 bitlap.org .
 */
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
trait TableFormatProvider {
    def getMetricWriter(output: Path): BitlapWriter[MetricRow]
    def getMetricDimWriter(output: Path): BitlapWriter[MetricDimRow]

    def getMetricMetaReader(
        dataPath: Path,
        timeFunc: TimeFilterFun,
        metrics: List[String],
        projections: List[String]
    ): BitlapReader[MetricRowMeta]

    def getMetricReader(
        dataPath: Path,
        timeFunc: TimeFilterFun,
        metrics: List[String],
        projections: List[String]
    ): BitlapReader[MetricRow]

    def getMetricDimMetaReader(
        dataPath: Path,
        timeFunc: TimeFilterFun,
        metrics: List[String],
        projections: List[String],
        dimension: String,
        dimensionFilter: PrunePushedFilter
    ): BitlapReader[MetricDimRowMeta]

    def getMetricDimReader(
        dataPath: Path,
        timeFunc: TimeFilterFun,
        metrics: List[String],
        projections: List[String],
        dimension: String,
        dimensionFilter: PrunePushedFilter
    ): BitlapReader[MetricDimRow]
}
