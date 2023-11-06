/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.core.storage.io

import org.bitlap.common.utils.JsonUtil
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.BitlapReader
import org.bitlap.core.storage.BitlapReaders
import org.bitlap.core.storage.BitlapWriter
import org.bitlap.core.storage.TableFormatProvider
import org.bitlap.core.storage.io
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta
import org.bitlap.roaringbitmap.x.BBM
import org.bitlap.roaringbitmap.x.CBM

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HiddenFileFilter

/** Implementation of parquet table format
 */
class BitlapParquetProvider(val table: Table, private val fs: FileSystem) extends TableFormatProvider {

  import BitlapParquetProvider._

  override def getMetricWriter(output: Path): BitlapWriter[MetricRow] = {
    BitlapParquetWriter(
      fs,
      output,
      METRIC_SCHEMA,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      { row =>
        val r = GenericData.Record(METRIC_SCHEMA)
        r.put(0, row.metricKey)
        r.put(1, row.tm)
        r.put(2, row.metric.getBytes())
        r.put(3, row.entity.getBytes())
        r.put(4, JsonUtil.json(row.metadata))
        r
      }
    )
  }

  override def getMetricDimWriter(output: Path): BitlapWriter[MetricDimRow] = {
    BitlapParquetWriter(
      fs,
      output,
      METRIC_DIM_SCHEMA,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      { row =>
        val r = GenericData.Record(METRIC_DIM_SCHEMA)
        r.put(0, row.metricKey)
        r.put(1, row.dimensionKey)
        r.put(2, row.dimension)
        r.put(3, row.tm)
        r.put(4, row.metric.getBytes())
        r.put(5, row.entity.getBytes())
        r.put(6, JsonUtil.json(row.metadata))
        r
      }
    )
  }

  override def getMetricMetaReader(
    dataPath: Path,
    timeFunc: TimeFilterFun,
    metrics: List[String],
    projections: List[String]
  ): BitlapReader[MetricRowMeta] = {
    val inputs              = this.listFilePath(dataPath, timeFunc)
    val metricFilter        = BitlapReaders.makeParquetFilter("mk", metrics)
    val requestedProjection = BitlapReaders.makeAvroSchema(METRIC_SCHEMA, projections)
    BitlapParquetReader(
      fs,
      inputs,
      METRIC_SCHEMA,
      requestedProjection,
      metricFilter.compact(),
      { row =>
        val metaObj = JsonUtil.jsonAs(row.getWithDefault("meta", "{}"), classOf[MetricRowMeta])
        if (timeFunc(metaObj.tm)) {
          metaObj
        } else {
          null
        }
      }
    )
  }

  override def getMetricReader(
    dataPath: Path,
    timeFunc: TimeFilterFun,
    metrics: List[String],
    projections: List[String]
  ): BitlapReader[MetricRow] = {
    val inputs              = this.listFilePath(dataPath, timeFunc)
    val metricFilter        = BitlapReaders.makeParquetFilter("mk", metrics)
    val requestedProjection = BitlapReaders.makeAvroSchema(METRIC_SCHEMA, projections)
    BitlapParquetReader(
      fs,
      inputs,
      METRIC_SCHEMA,
      requestedProjection,
      metricFilter.compact(),
      { row =>
        val tm = row.get("t")
        if (tm != null && timeFunc(tm.asInstanceOf[Long])) {
          new MetricRow(
            tm.asInstanceOf[Long],
            row.getWithDefault("mk", ""),
            CBM(row.getWithDefault("m", Array.empty[Byte])),
            BBM(row.getWithDefault("e", Array.empty[Byte]))
          )
        } else {
          null
        }
      }
    )
  }

  override def getMetricDimMetaReader(
    dataPath: Path,
    timeFunc: TimeFilterFun,
    metrics: List[String],
    projections: List[String],
    dimension: String,
    dimensionFilter: PrunePushedFilter
  ): BitlapReader[MetricDimRowMeta] = {
    val inputs              = this.listFilePath(dataPath, timeFunc)
    val filter              = this.getMetricDimFilter(metrics, dimension, dimensionFilter)
    val requestedProjection = BitlapReaders.makeAvroSchema(METRIC_DIM_SCHEMA, projections)
    BitlapParquetReader(
      fs,
      inputs,
      METRIC_DIM_SCHEMA,
      requestedProjection,
      filter.compact(),
      { row =>
        val metaObj = JsonUtil.jsonAs(row.getWithDefault("meta", "{}"), classOf[MetricDimRowMeta])
        if (timeFunc(metaObj.tm)) {
          metaObj
        } else {
          null
        }
      }
    )
  }

  override def getMetricDimReader(
    dataPath: Path,
    timeFunc: TimeFilterFun,
    metrics: List[String],
    projections: List[String],
    dimension: String,
    dimensionFilter: PrunePushedFilter
  ): BitlapReader[MetricDimRow] = {
    val inputs              = this.listFilePath(dataPath, timeFunc)
    val filter              = this.getMetricDimFilter(metrics, dimension, dimensionFilter)
    val requestedProjection = BitlapReaders.makeAvroSchema(METRIC_DIM_SCHEMA, projections)
    BitlapParquetReader(
      fs,
      inputs,
      METRIC_DIM_SCHEMA,
      requestedProjection,
      filter.compact(),
      { row =>
        val tm = row.get("t")
        if (tm != null && timeFunc(tm.asInstanceOf[Long])) {
          new MetricDimRow(
            tm.asInstanceOf[Long],
            row.getWithDefault("mk", ""),
            row.getWithDefault("dk", ""),
            row.getWithDefault("d", ""),
            CBM(row.getWithDefault("m", Array.empty[Byte])),
            BBM(row.getWithDefault("e", Array.empty[Byte]))
          )
        } else {
          null
        }
      }
    )
  }

  private def getMetricDimFilter(metrics: List[String], dimension: String, dimensionFilter: PrunePushedFilter)
    : FilterPredicate = {
    var filter = BitlapReaders.makeParquetFilterAnd(
      List(
        "mk" -> metrics,
        "dk" -> List(dimension)
      )
    )
    val dimFilter = BitlapReaders.makeParquetFilterFromPrunePushedFilter(dimensionFilter, "d")
    if (dimFilter != null) {
      filter = FilterApi.and(filter, dimFilter)
    }
    filter
  }

  // TODO (get partitions)
  private def listFilePath(dataPath: Path, timeFunc: TimeFilterFun): List[Path] = {
    fs
      .listStatus(dataPath)
      .filter(it => it.isDirectory && timeFunc(it.getPath.getName.split("=")(1).toLong))
      .flatMap(filePath => fs.listStatus(filePath.getPath, HiddenFileFilter.INSTANCE).map(_.getPath))
      .toList
  }
}

object BitlapParquetProvider {

  // TODO (add enum & add shard_id if cbm is too big)
  val METRIC_SCHEMA: Schema = SchemaBuilder
    .builder()
    .record("metric")
    .namespace(classOf[BitlapParquetProvider].getPackageName)
    .fields()
    .optionalString("mk")
    .optionalLong("t")
    // .optionalString(ek)
    // .name("m").type().array().items().bytesType().noDefault()
    // .name("e").type().array().items().bytesType().noDefault()
    .optionalBytes("m")
    .optionalBytes("e")
    .optionalString("meta")
    .endRecord()

  val METRIC_DIM_SCHEMA: Schema = SchemaBuilder
    .builder()
    .record("metric")
    .namespace(classOf[BitlapParquetProvider].getPackageName)
    .fields()
    .optionalString("mk")
    .optionalString("dk")
    .optionalString("d")
    .optionalLong("t")
    // .optionalString(ek)
    // .name("m").type().array().items().bytesType().noDefault()
    // .name("e").type().array().items().bytesType().noDefault()
    .optionalBytes("m")
    .optionalBytes("e")
    .optionalString("meta")
    .endRecord()
}
