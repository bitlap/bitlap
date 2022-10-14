/* Copyright (c) 2022 bitlap.org */
package io.bitlap
import io.bitlap.spark.SparkOperatorLive._
import org.apache.spark.sql._
import zio.{ TypeTag => _, _ }

import java.util._

/** @since 2022/10/14
 *  @author
 *    梦境迷离
 */
package object spark {
  implicit final class DataFrameOps(val dataFrame: DataFrame) extends AnyVal {
    def writeDF(url: String, table: String, connectionProperties: Properties): Task[Unit] =
      ZIO
        .service[DataFrame]
        .map(_.write.format(FORMAT).jdbc(url, table, connectionProperties)) // options ?
        .provideLayer(ZLayer.succeed(dataFrame))
  }
}
