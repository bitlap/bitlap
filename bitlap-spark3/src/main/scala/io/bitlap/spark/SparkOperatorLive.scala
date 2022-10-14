/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark
import org.apache.spark.sql.{ DataFrame, SparkSession }
import zio.{ TypeTag => _, _ }
import java.util.Properties
import io.bitlap.spark.SparkOperatorLive._
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

  override def readDF(url: String, table: String, properties: Properties): Task[DataFrame] =
    ZIO
      .service[SparkSession]
      .map(_.read.format(FORMAT).jdbc(url, table, properties))
      .provideLayer(live)
}
object SparkOperatorLive {

  final val FORMAT: String = "io.bitlap.spark"

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
