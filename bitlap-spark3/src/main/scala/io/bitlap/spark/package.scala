/* Copyright (c) 2022 bitlap.org */
package io.bitlap

import org.apache.spark.sql._
import zio.{ TypeTag => _, _ }

/** @since 2022/10/14
 *  @author
 *    梦境迷离
 */
package object spark {

  val FORMAT: String = "bitlap"

  implicit final class DataFrameOps(val dataFrame: DataFrame) extends AnyVal {
    def saveToBitlap(options: Map[String, String]): Task[Unit] =
      ZIO
        .service[DataFrame]
        .map(_.write.format(FORMAT).options(options).save())
        .provideLayer(ZLayer.succeed(dataFrame))
  }
}
