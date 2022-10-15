/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark

import io.bitlap.spark.SparkOperatorLive._
import org.apache.spark.sql._
import zio.{ TypeTag => _, _ }

import scala.reflect.runtime.universe.TypeTag

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/14
 */
final case class SparkOperatorLive() extends SparkOperator[Task] {

  override def createDataFrame[T <: SparkData: TypeTag](sqlData: List[T]): Task[DataFrame] =
    ZIO.service[SparkSession].map(_.createDataFrame(sqlData)).provideLayer(live)

  override def activeSparkSession(): Task[SparkSession] =
    ZIO.effect(SparkSession.active)

  override def read(url: String, options: Map[String, String]): Task[DataFrame] =
    ZIO
      .service[SparkSession]
      .map(_.read.format(FORMAT).options(options).load())
      .provideLayer(live)
}
object SparkOperatorLive {

  lazy val session: SparkSession = SparkSession
    .builder()
    .appName("bitlap")
    .master("local[*,2]") // config
    .getOrCreate()

  lazy val live: ZLayer[Any, Throwable, Has[SparkSession]] =
    ZLayer.fromAcquireRelease(
      ZIO.effect(
        session
      )
    )(release => ZIO.effect(release.stop()).ignore)

}
