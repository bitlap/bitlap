/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark
import org.apache.spark.sql.{ DataFrame, SparkSession }
import zio._
import java.util.Properties
import scala.reflect.runtime.universe.TypeTag

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/14
 */
trait SparkOperator[F[_]] {

  def activeSparkSession(): F[SparkSession]

  def createDataFrame[T <: SparkData: TypeTag](sqlData: List[T]): F[DataFrame]

  def jdbcRead(url: String, table: String, properties: Properties): F[DataFrame]
}

object SparkOperator {

  def createDataFrame[T <: SparkData](sqlData: List[T]): Task[DataFrame] =
    ZIO
      .serviceWith[SparkOperator[Task]](_.createDataFrame(sqlData))
      .provideLayer(ZLayer.succeed[SparkOperator[Task]](SparkOperatorLive()))

  def activeSparkSession(): Task[SparkSession] =
    ZIO
      .serviceWith[SparkOperator[Task]](_.activeSparkSession())
      .provideLayer(ZLayer.succeed[SparkOperator[Task]](SparkOperatorLive()))

  def jdbcRead(url: String, table: String, properties: Properties): Task[DataFrame] =
    ZIO
      .serviceWith[SparkOperator[Task]](_.jdbcRead(url, table, properties))
      .provideLayer(ZLayer.succeed[SparkOperator[Task]](SparkOperatorLive()))
}
