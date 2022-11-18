/* Copyright (c) 2022 bitlap.org */
package org.bitlap

import org.apache.spark.sql._
import zio._

/** @since 2022/10/14
 *  @author
 *    梦境迷离
 */
package object spark {

  final val FORMAT: String = "bitlap"

  Class.forName(classOf[Driver].getName)

  implicit final class DataFrameOps(val dataFrame: DataFrame) extends AnyVal {
    def liftDataFrameWriter: Task[DataFrameWriter[Row]] =
      ZIO.effect(dataFrame.write.format(FORMAT))
  }

  implicit final class SparkSessionOps(val sparkSession: SparkSession) extends AnyVal {
    def liftDataFrameReader: Task[DataFrameReader] =
      ZIO.effect(sparkSession.read.format(FORMAT))
  }
}
