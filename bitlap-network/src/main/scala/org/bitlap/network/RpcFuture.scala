/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ FetchResults, TableSchema }

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/** Rpc api and monad for future backend.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait RpcFuture extends Rpc[Future] {

  def pure[A](a: A): Future[A] = Future.successful(a)

  def map[A, B](fa: Future[A])(f: A => B): Future[B] = fa.map(f)

  def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)

  def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Future[SessionHandle]

  def closeSession(sessionHandle: SessionHandle): Future[Unit]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): Future[OperationHandle]

  def fetchResults(opHandle: OperationHandle): Future[FetchResults]

  def getResultSetMetadata(opHandle: OperationHandle): Future[TableSchema]

  def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String = null,
    tableName: String = null,
    columnName: String = null
  ): Future[OperationHandle]

  def getDatabases(pattern: String): Future[OperationHandle]

  def getTables(database: String, pattern: String): Future[OperationHandle]

  def getSchemas(sessionHandle: SessionHandle, catalogName: String, schemaName: String): Future[OperationHandle]
}
