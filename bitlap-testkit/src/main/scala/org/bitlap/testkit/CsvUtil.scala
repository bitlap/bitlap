/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import org.bitlap.csv._
import java.io._

/** csv 读写工具
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/5/2
 */
trait CsvUtil {

  implicit val format = new DefaultCsvFormat {
    override def prependHeader: List[String] = List("time", "entity", "dimensions", "metric_name", "metric_value")
    override def ignoreHeader: Boolean       = true
  }

  def readCsvData(resource: String): List[Metric] = {
    val reader = ClassLoader.getSystemResourceAsStream(resource)
    readCsvData(reader)
  }

  def readCsvData(resource: InputStream): List[Metric] =
    ScalableBuilder[Metric]
      .setField[List[Dimension]](
        _.dimensions,
        dims => StringUtils.extractJsonValues[Dimension](dims)((k, v) => Dimension(k, v))
      )
      .convertFrom(resource)
      .collect { case Some(v) => v }

  def writeCsvData(file: File, metrics: List[Metric]): Boolean =
    CsvableBuilder[Metric]
      .setField[List[Dimension]](
        _.dimensions,
        (ds: List[Dimension]) =>
          s"""\"{${ds.map(kv => s"""\"\"${kv.key}\"\":\"\"${kv.value}\"\"""").mkString(",")}}\""""
      )
      .convertTo(metrics, file)
}
