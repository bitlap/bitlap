/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.testkit

import java.io.*

import bitlap.rolls.csv.*
import bitlap.rolls.csv.CSVUtils.*

/** csv utils
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/5/2
 */
trait CSVUtils {

  given CSVFormat = new CSVFormat {
    override val hasHeaders: Boolean  = true
    override val hasColIndex: Boolean = false
  }

  def readCSVData(file: File): List[Metric] =
    val (metadata, metrics) = CSVUtils.readCSV(
      FileName(file.getName)
    ) { line =>
      line
        .into[Metric]
        .withFieldComputed(_.dimensions, dims => StringUtils.asClasses(dims)((k, v) => Dimension(k, v)))
        .decode
    }
    metrics.toList

  def readClasspathCSVData(file: String): List[Metric] =
    val (metadata, metrics) = CSVUtils.readCSV(
      FileName(this.getClass.getClassLoader.getResource(file).getFile)
    ) { line =>
      line
        .into[Metric]
        .withFieldComputed(_.dimensions, dims => StringUtils.asClasses(dims)((k, v) => Dimension(k, v)))
        .decode
    }
    metrics.toList

  def writeCSVData(file: File, metrics: List[Metric]): Boolean =
    val status = CSVUtils.writeCSV(file, metrics) { m =>
      m.into
        .withFieldComputed(_.dimensions, dims => StringUtils.asString(dims.map(f => f.key -> f.value).toList))
        .encode
    }
    status
}
