/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import org.bitlap.csv.core.{ ScalableBuilder, ScalableHelper, StringUtils }

import java.io.{ BufferedReader, InputStreamReader }
import scala.collection.mutable.ListBuffer
import scala.util.Using

/**
 * @author 梦境迷离
 * @version 1.0,2022/5/2
 */
trait CsvHelper {

  def readCsvFromClassPath[T <: Product](fileName: String)(func: String => Option[T]): List[Option[T]] = {
    val ts = ListBuffer[Option[T]]()
    val reader = new InputStreamReader(ClassLoader.getSystemResourceAsStream(fileName))
    val bufferedReader = new BufferedReader(reader)
    var line: String = null
    Using.resource(bufferedReader) { input =>
      while ({
        line = input.readLine()
        line != null
      })
        ts.append(func(line))
    }

    ts.result()
  }

  def readCsvData(resourceFileName: String): List[Metric] =
    readCsvFromClassPath[Metric](resourceFileName) { line =>
      ScalableBuilder[Metric]
        .setField[List[Dimension]](
          _.dimensions,
          dims => StringUtils.extractJsonValues[Dimension](dims)((k, v) => Dimension(k, v))
        )
        .build(line)
        .toScala
    }.collect { case Some(v) => v }
}
