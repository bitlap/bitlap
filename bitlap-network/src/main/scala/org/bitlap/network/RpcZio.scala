/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ FetchResults, TableSchema }
import zio.Task
import zio.ZIO

/** Rpc api and monad for zio backend.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait RpcZio extends Rpc[Task] { self =>

  override def pure[A](a: A): Task[A] = Task.succeed(a)

  override def map[A, B](fa: self.type => Task[A])(f: A => B): Task[B] = fa(this).map(f)

  override def flatMap[A, B](fa: self.type => Task[A])(f: A => Task[B]): Task[B] = fa(this).flatMap(f)

  def sync[T, Z <: ZIO[_, _, _]](action: => Z)(implicit runtime: zio.Runtime[Any] = zio.Runtime.default): T =
    runtime.unsafeRun(action.asInstanceOf[ZIO[Any, Throwable, T]])

  def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Task[SessionHandle]

  def closeSession(sessionHandle: SessionHandle): Task[Unit]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): Task[OperationHandle]

  def fetchResults(opHandle: OperationHandle, maxRows: Int, fetchType: Int): Task[FetchResults]

  def getResultSetMetadata(opHandle: OperationHandle): Task[TableSchema]

  def getColumns(
    sessionHandle: SessionHandle,
    tableName: String,
    schemaName: String,
    columnName: String
  ): Task[OperationHandle]

  def getDatabases(pattern: String): Task[OperationHandle]

  def getTables(database: String, pattern: String): Task[OperationHandle]

  def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): Task[OperationHandle]

}
