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
package org.bitlap.core.mdm

import java.io.{ Closeable, InputStream, InputStreamReader, Serializable }

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import org.bitlap.common.*
import org.bitlap.common.data.*
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.utils.{ JsonUtil, StringEx }
import org.bitlap.common.utils.StringEx.*
import org.bitlap.core.*
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.sql.QueryContext
import org.bitlap.core.storage.BitlapStore
import org.bitlap.core.storage.load.{ MetricDimRow, MetricDimRowMeta, MetricRow, MetricRowMeta }
import org.bitlap.roaringbitmap.x.{ BBM, CBM }

import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

/** bitlap mdm [[Event]] writer
 */
class BitlapEventWriter(val table: Table, hadoopConf: Configuration) extends Serializable with Closeable {

  @volatile
  private var closed = false
  private val log    = LoggerFactory.getLogger(classOf[BitlapEventWriter])
  private val store  = BitlapStore(table, hadoopConf).open()

  /** write mdm events
   */
  def write(events: List[Event]): Unit = {
    this.checkOpen()
    log.info(s"Start writing ${events.size} events.")
    if (events.isEmpty) {
      return
    }
    val cost = elapsed {
      QueryContext.use { ctx =>
        ctx.queryId = StringEx.uuid(true)
        this.write0(events)
      }
    }
    log.info { s"End writing ${events.size} events, elapsed ${cost}ms." }
  }

  /** write mdm events from csv
   */
  def writeCsv(input: InputStream): Unit = {
    val headers = Event.schema.map(_._1)
    val parser  = new CSVParser(new InputStreamReader(input), CSVFormat.DEFAULT.withFirstRecordAsHeader)
    val events  = ListBuffer[Event]()
    for (row <- parser.asScala) {
      if (row.values().forall(!_.nullOrBlank)) {
        events += Event.of(
          row.get(0).toLong,
          Entity(row.get(1).toInt, "ENTITY"),
          Dimension(JsonUtil.jsonAsMap(row.get(2))),
          Metric(row.get(3), row.get(4).toDouble)
        )
      }
    }
    this.write(events.toList)
  }

  private def write0(events: List[Event]): Unit = {
    events.groupBy(_.time).foreach { case (time, rs) =>
      // 1. agg metric
      val singleRows = rs.groupBy { it => (it.entity, it.dimension, it.metric.key) }.map {
        case ((entity, dimension, metricKey), groupRows) =>
          new EventWithDimId(time, entity, dimension, new Metric(metricKey, groupRows.map(_.metric.value).sum), 0)
      }.toList
      // 2. identify sort id for dimensions for each entity + metric
      val cleanRows = singleRows.groupBy { it => s"${it.entity}${it.metric.key}" }.flatMap { case (_, sRows) =>
        val temps   = mutable.Map[String, Int]()
        var counter = 0

        sRows.sortBy(-_.metric.value).map { sRow =>
          val dim = sRow.dimension.toString
          if (temps.contains(dim)) {
            sRow.dimId = temps(dim)
          } else {
            sRow.dimId = counter
            temps(dim) = counter
            counter += 1
          }
          sRow
        }
      }
      // store metrics
      val metricRows =
        cleanRows.groupBy { it => (it.entity.key, it.metric.key) }.map { case ((entityKey, metricKey), groupRows) =>
          val bbm = BBM()
          val cbm = CBM()
          groupRows.foreach { gr =>
            bbm.add(gr.dimId, gr.entity.id)
            cbm.add(gr.dimId, gr.entity.id, gr.metric.value.toLong) // // TODO double support
          }
          MetricRow(
            time,
            metricKey,
            cbm,
            bbm,
            MetricRowMeta(time, metricKey, bbm.getCountUnique, bbm.getLongCount, cbm.getCount)
          )
        }.toList
      store.storeMetric(time, metricRows)

      // store metric with one dimension
      val metricDimRows = cleanRows.flatMap { r =>
        r.dimension.dimensions.map { case (key, value) =>
          r.copy(r.time, r.entity, Dimension(Map(key -> value)), r.metric, r.dimId)
        }
      }.groupBy { it => (it.entity.key, it.metric.key, it.dimension.firstPair()) }.map {
        case ((entityKey, metricKey, dimension), groupRows) =>
          val dk  = dimension._1
          val d   = dimension._2
          val bbm = BBM()
          val cbm = CBM()
          groupRows.foreach { gr =>
            bbm.add(gr.dimId, gr.entity.id)
            cbm.add(gr.dimId, gr.entity.id, gr.metric.value.toLong) // // TODO double support
          }
          MetricDimRow(
            time,
            metricKey,
            dk,
            d,
            cbm,
            bbm,
            MetricDimRowMeta(time, metricKey, dk, d, bbm.getCountUnique, bbm.getLongCount, cbm.getCount)
          )
      }.toList
      store.storeMetricDim(time, metricDimRows)

    // store metric with high cardinality dimensions
    // TODO
    }
  }

  private def checkOpen(): Unit = {
    if (closed) {
      throw BitlapException(s"BitlapWriter has been closed.")
    }
  }

  override def close(): Unit = {
    closed = true
    try {
      this.store.close()
    } catch {
      case NonFatal(e) =>
        log.error(s"Error when closing BitlapWriter, cause: ", e)
    }
  }
}
