/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.testkit

import java.io.*

import scala.util.Using

import bitlap.rolls.csv.*
import bitlap.rolls.csv.CSVUtils.*

/** CSV utils
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

  def checkCSVData(file: File)(expect: List[Metric] => Boolean): Boolean =
    val (metadata, metrics) = CSVUtils.readCSV(
      FileName(file.getPath)
    ) { line =>
      line
        .into[Metric]
        .withFieldComputed(_.dimensions, dims => StringUtils.asClasses(dims)((k, v) => Dimension(k, v)))
        .decode
    }
    expect(metrics.toList)

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
    if (file.exists()) file.delete() else file.createNewFile()
    val res = CSVUtils.writeCSV(file, metrics) { m =>
      m.into
        .withFieldComputed(_.dimensions, dims => StringUtils.asString(dims.map(f => f.key -> f.value).toList))
        .encode
    }
    res
}
