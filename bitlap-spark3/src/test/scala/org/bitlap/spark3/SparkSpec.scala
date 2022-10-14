/* Copyright (c) 2022 bitlap.org */
package org.bitlap.spark3

import io.bitlap.spark.SparkData._
import io.bitlap.spark._
import junit.framework._

/** @since 2022/10/14
 *  @author
 *    梦境迷离
 */
class SparkSpec extends TestCase("SparkSpec") {

  val url = "jdbc:bitlap://localhost:23333/default"
//
//  // is OK ?
//  def write(): Unit =
//    for {
//      r <- SparkOperator.createDataFrame(List(Dimension("", "")))
//      _ <- SparkOperator.write(r)(url, "table", null)
//    } yield ()
//
//  def read(): Unit =
//    for {
//      df <- SparkOperator.read(url, "table", null)
//    } yield df.show()

}
