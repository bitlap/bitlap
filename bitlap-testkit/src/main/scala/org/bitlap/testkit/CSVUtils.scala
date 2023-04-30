/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit

import bitlap.rolls.csv.*
import bitlap.rolls.csv.CSVUtils.*
import java.io.*

/** csv 读写工具
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/5/2
 */
trait CSVUtils {

  given CSVFormat = new CSVFormat {
    override val hasHeaders: Boolean  = false
    override val hasColIndex: Boolean = false
  }

  def readCsvData(file: String): List[Metric] =
    val (metadata, metrics) = CSVUtils.readCSV(
      FileName(this.getClass.getClassLoader.getResource(file).getFile)
    ) { line =>
      line
        .into[Metric]
        .withFieldComputed(_.dimensions, dims => StringUtils.asClasses(dims)((k, v) => Dimension(k, v)))
        .decode
    }
    metrics.toList

  def writeCsvData(file: File, metrics: List[Metric]): Boolean =
    val status = CSVUtils.writeCSV(file, metrics) { m =>
      m.into
        .withFieldComputed(_.dimensions, dims => StringUtils.asString(dims.map(f => f.key -> f.value).toList))
        .encode
    }
    status
}
