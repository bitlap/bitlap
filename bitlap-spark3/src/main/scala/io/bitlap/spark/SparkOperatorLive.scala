/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark
import org.apache.spark.sql.{ DataFrame, SparkSession }
import zio._
import java.util.Properties
import io.bitlap.spark.SparkOperatorLive._

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/14
 */
final case class SparkOperatorLive() extends SparkOperator[Task] {

  override def createDataFrame[T <: SparkData](sqlData: List[T]): Task[DataFrame] =
    ZIO.service[SparkSession].map(_.createDataFrame(sqlData)).provideLayer(live)

  override def activeSparkSession(): Task[SparkSession] =
    ZIO.effect(SparkSession.active)

  override def dataFrame(url: String, table: String, properties: Properties): Task[DataFrame] =
    ZIO
      .service[SparkSession]
      .map(_.read.format(FORMAT).jdbc(url, table, properties))
      .provideLayer(live)

  override def write(dataFrame: DataFrame)(url: String, table: String, connectionProperties: Properties): Task[Unit] =
    ZIO
      .service[DataFrame]
      .map(_.write.format(FORMAT).jdbc(url, table, connectionProperties))
      .provideLayer(ZLayer.succeed(dataFrame))

}
object SparkOperatorLive {

  final val FORMAT: String = "io.bitlap.spark"

  lazy val session: SparkSession = SparkSession
    .builder()
    .appName("bitlap")
    .master("local[*,2]")
    .getOrCreate()

  lazy val live: ZLayer[Any, Throwable, Has[SparkSession]] =
    ZLayer.fromAcquireRelease(
      ZIO.effect(
        session
      )
    )(release => ZIO.effect(release.stop()).ignore)

}
