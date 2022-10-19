/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.jdbc
import zio._

import java.sql._

final case class JdbcClientOperatorLive() extends JdbcClientOperator {
  override def getConnection(url: String): Task[Statement] =
    ZIO
      .serviceWith[Connection](c => ZIO.effect(c.createStatement()))
      .provideLayer(JdbcClientOperatorLive.connectionLive(url))

  def getDatabaseMetaData(url: String): Task[DatabaseMetaData] =
    ZIO
      .serviceWith[Connection](c => ZIO.effect(c.getMetaData))
      .provideLayer(JdbcClientOperatorLive.connectionLive(url))
}

object JdbcClientOperatorLive {

  lazy val live: ULayer[Has[JdbcClientOperator]] = ZLayer.succeed(JdbcClientOperatorLive())

  Class.forName(classOf[Driver].getName)

  def connectionLive(url: String): ZLayer[Any, Throwable, Has[Connection]] =
    ZLayer.fromAcquireRelease(
      ZIO.effect(
        DriverManager.getConnection(url)
      )
    )(release => ZIO.effect(release.close()).ignore)

}
