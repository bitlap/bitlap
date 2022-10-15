/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark
import org.apache.spark.sql._
import zio._

import scala.reflect.runtime.universe.TypeTag

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/14
 */
trait SparkOperator[F[_]] {

  def activeSparkSession(): F[SparkSession]

  def createDataFrame[T <: SparkData: TypeTag](sqlData: List[T]): F[DataFrame]

  def read(url: String, options: Map[String, String]): F[DataFrame]
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

  def read(url: String, options: Map[String, String]): Task[DataFrame] =
    ZIO
      .serviceWith[SparkOperator[Task]](_.read(url, options))
      .provideLayer(ZLayer.succeed[SparkOperator[Task]](SparkOperatorLive()))
}
