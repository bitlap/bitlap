/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.jdbc

import zio._

import java.sql._

/** @since 2022/10/14
 *  @author
 *    梦境迷离
 */
trait JdbcClientOperator {
  def getConnection(url: String): Task[Statement]
  def getDatabaseMetaData(url: String): Task[DatabaseMetaData]
}
object JdbcClientOperator {
  def getDatabaseMetaData(url: String): Task[DatabaseMetaData] =
    ZIO.serviceWith[JdbcClientOperator](_.getDatabaseMetaData(url)).provideLayer(JdbcClientOperatorLive.live)

  def getConnection(url: String): Task[Statement] =
    ZIO.serviceWith[JdbcClientOperator](_.getConnection(url)).provideLayer(JdbcClientOperatorLive.live)
}
