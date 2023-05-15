/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit

import org.bitlap.common.data.Event
import org.bitlap.csv._

import java.io._
import scala.jdk.CollectionConverters.ListHasAsScala

/** csv 读写工具
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/5/2
 */
trait CsvUtil {

  implicit val format = new DefaultCsvFormat {
    override def prependHeader: List[String] = Event.getSchema.asScala.map(_.getFirst).toList
    override def ignoreHeader: Boolean       = true
  }

  def readCsvData(resource: String): List[Metric] = {
    val reader = ClassLoader.getSystemResourceAsStream(resource)
    readCsvData(reader)
  }

  def readCsvData(resource: InputStream): List[Metric] =
    ReaderBuilder[Metric]
      .setField[List[Dimension]](
        _.dimensions,
        dims => StringUtils.extractJsonValues[Dimension](dims)((k, v) => Dimension(k, v))
      )
      .convertFrom(resource)
      .collect { case Some(v) => v }

  def writeCsvData(file: File, metrics: List[Metric]): Boolean =
    WriterBuilder[Metric]
      .setField[List[Dimension]](
        _.dimensions,
        (ds: List[Dimension]) =>
          s"""\"{${ds.map(kv => s"""\"\"${kv.key}\"\":\"\"${kv.value}\"\"""").mkString(",")}}\""""
      )
      .convertTo(metrics, file)
}
