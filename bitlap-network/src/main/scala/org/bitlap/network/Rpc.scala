/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ FetchResults, TableSchema }

import io.grpc.Status
import zio.ZIO

import scala.concurrent.Future
import zio.IO

/** Rpc api and monad for backends.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait Rpc[F[_]] {

  def pure[A](a: A): F[A]

  def map[A, B](fa: F[A])(f: A => B): F[B]

  def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]

  def transformZIO[R, A, B](r: R)(fx: R => Future[A])(f: A => B): ZIO[Any, Status, B] =
    IO.fromFuture[A](make => fx(r))
      .map(hd => f(hd))
      .mapError(errorApplyFunc)

  def pure[T](action: => T): ZIO[Any, Status, T] =
    IO.effect(action).mapError { ex => ex.printStackTrace(); Status.fromThrowable(ex) }

  def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): F[SessionHandle]

  def closeSession(sessionHandle: SessionHandle): F[Unit]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): F[OperationHandle]

  def fetchResults(opHandle: OperationHandle): F[FetchResults]

  def getResultSetMetadata(opHandle: OperationHandle): F[TableSchema]

  def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String = null,
    tableName: String = null,
    columnName: String = null
  ): F[OperationHandle]

  /** get databases or schemas from catalog
   *
   *  @see
   *    `show databases`
   */
  def getDatabases(pattern: String): F[OperationHandle]

  /** get tables from catalog with database name
   *
   *  @see
   *    `show tables in [db_name]`
   */
  def getTables(database: String, pattern: String): F[OperationHandle]

  /** get schemas from catalog
   *
   *  @see
   *    `show tables in [db_name]`
   */
  def getSchemas(sessionHandle: SessionHandle, catalogName: String, schemaName: String): F[OperationHandle]
}
