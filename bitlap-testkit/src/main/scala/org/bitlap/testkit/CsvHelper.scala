/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import org.bitlap.csv.{ ScalableBuilder, StringUtils }

/** @author
 *    梦境迷离
 *  @version 1.0,2022/5/2
 */
trait CsvHelper {

  def readCsvData(resourceFileName: String): List[Metric] = {
    val reader = ClassLoader.getSystemResourceAsStream(resourceFileName)
    ScalableBuilder[Metric]
      .setField[List[Dimension]](
        _.dimensions,
        dims => StringUtils.extractJsonValues[Dimension](dims)((k, v) => Dimension(k, v))
      )
      .convertFrom(reader)
  }.collect { case Some(v) => v }
}
