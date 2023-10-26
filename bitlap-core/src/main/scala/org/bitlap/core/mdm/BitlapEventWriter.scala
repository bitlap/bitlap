/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm

import java.io.{ Closeable, InputStream, InputStreamReader, Serializable }

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.data.Dimension
import org.bitlap.common.data.Entity
import org.bitlap.common.data.Event
import org.bitlap.common.data.EventWithDimId
import org.bitlap.common.data.Metric
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.utils.StringEx
import org.bitlap.core.*
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.sql.QueryContext
import org.bitlap.core.storage.BitlapStore
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta
import org.bitlap.core.utils.JsonUtil

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
    val headers = Event.getSchema.asScala.map(_.getFirst)
    val parser  = new CSVParser(new InputStreamReader(input), CSVFormat.DEFAULT.withFirstRecordAsHeader)
    val events  = ListBuffer[Event]()
    for (row <- parser.asScala) {
      if (row.values().forall(!StringEx.nullOrBlank(_))) {
        events += Event.of(
          row.get(0).toLong,
          Entity(row.get(1).toInt, "ENTITY"),
          new Dimension(JsonUtil.jsonAsMap(row.get(2)).asJava),
          Metric(row.get(3), row.get(4).toDouble)
        )
      }
    }
    this.write(events.toList)
  }

  private def write0(events: List[Event]): Unit = {
    events.groupBy(_.getTime).foreach { case (time, rs) =>
      // 1. agg metric
      val singleRows = rs.groupBy { it => (it.getEntity, it.getDimension, it.getMetric.getKey) }.map {
        case ((entity, dimension, metricKey), groupRows) =>
          new EventWithDimId(time, entity, dimension, new Metric(metricKey, groupRows.map(_.getMetric.getValue).sum), 0)
      }.toList
      // 2. identify sort id for dimensions for each entity + metric
      val cleanRows = singleRows.groupBy { it => s"${it.getEntity}${it.getMetric.getKey}" }.flatMap { case (_, sRows) =>
        val temps   = mutable.Map[String, Int]()
        var counter = 0

        sRows.sortBy(-_.getMetric.getValue).map { sRow =>
          val dim = sRow.getDimension.toString()
          if (temps.contains(dim)) {
            sRow.setDimId(temps(dim))
          } else {
            sRow.setDimId(counter)
            temps(dim) = counter
            counter += 1
          }
          sRow
        }
      }
      // store metrics
      val metricRows = cleanRows.groupBy { it => (it.getEntity.getKey, it.getMetric.getKey) }.map {
        case ((entityKey, metricKey), groupRows) =>
          val bbm = BBM()
          val cbm = CBM()
          groupRows.foreach { gr =>
            bbm.add(gr.getDimId, gr.getEntity.getId)
            cbm.add(gr.getDimId, gr.getEntity.getId, gr.getMetric.getValue.toLong) // // TODO double support
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
        r.getDimension.getDimensions.asScala.map { case (key, value) =>
          r.copy(r.getTime, r.getEntity, new Dimension(Map(key -> value).asJava), r.getMetric, r.getDimId)
        }
      }.groupBy { it => (it.getEntity.getKey, it.getMetric.getKey, it.getDimension.firstPair()) }.map {
        case ((entityKey, metricKey, dimension), groupRows) =>
          val dk  = dimension.getFirst
          val d   = dimension.getSecond
          val bbm = BBM()
          val cbm = CBM()
          groupRows.foreach { gr =>
            bbm.add(gr.getDimId, gr.getEntity.getId)
            cbm.add(gr.getDimId, gr.getEntity.getId, gr.getMetric.getValue.toLong) // // TODO double support
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

  private def checkOpen() = {
    if (closed) {
      throw BitlapException(s"BitlapWriter has been closed.")
    }
  }

  override def close() = {
    closed = true
    try {
      this.store.close()
    } catch {
      case NonFatal(e) =>
        log.error(s"Error when closing BitlapWriter, cause: ", e)
    }
  }
}
