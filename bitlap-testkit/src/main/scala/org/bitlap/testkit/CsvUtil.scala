/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import org.bitlap.csv.{ ScalableBuilder, StringUtils }
import org.bitlap.csv.DefaultCsvFormat
import org.bitlap.csv.CsvableBuilder
import java.io.File

/** @author
 *    梦境迷离
 *  @version 1.0,2022/5/2
 */
trait CsvUtil {

  implicit val format = new DefaultCsvFormat {
    override def prependHeader: List[String] = List("time", "entity", "dimensions", "metric_name", "metric_value")
    override def ignoreHeader: Boolean       = true
  }

  def readCsvData(resourceFileName: String): List[Metric] = {
    val reader = ClassLoader.getSystemResourceAsStream(resourceFileName)
    ScalableBuilder[Metric]
      .setField[List[Dimension]](
        _.dimensions,
        dims => StringUtils.extractJsonValues[Dimension](dims)((k, v) => Dimension(k, v))
      )
      .convertFrom(reader)
  }.collect { case Some(v) => v }

  def writeCsvData(file: File, metrics: List[Metric]): Boolean =
    CsvableBuilder[Metric]
      .setField[List[Dimension]](
        _.dimensions,
        (ds: List[Dimension]) =>
          s"""\"{${ds.map(kv => s"""\"\"${kv.key}\"\":\"\"${kv.value}\"\"""").mkString(",")}}\""""
      )
      .convertTo(metrics, file)
}
