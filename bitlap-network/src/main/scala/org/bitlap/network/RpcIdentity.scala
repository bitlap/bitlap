/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ FetchResults, TableSchema }

/** Rpc api and monad for sync backend.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait RpcIdentity extends Rpc[Identity] {

  def pure[A](a: A): Identity[A] = a

  def map[A, B](fa: Identity[A])(f: A => B): Identity[B] = f(fa)

  def flatMap[A, B](fa: Identity[A])(f: A => Identity[B]): Identity[B] = f(fa)

  def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Identity[SessionHandle]

  def closeSession(sessionHandle: SessionHandle): Identity[Unit]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): Identity[OperationHandle]

  def fetchResults(opHandle: OperationHandle, maxRows: Int, fetchType: Int): Identity[FetchResults]

  def getResultSetMetadata(opHandle: OperationHandle): Identity[TableSchema]

  def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String,
    tableName: String,
    columnName: String
  ): Identity[OperationHandle]

  def getDatabases(pattern: String): Identity[OperationHandle]

  def getTables(database: String, pattern: String): Identity[OperationHandle]

  def getSchemas(sessionHandle: SessionHandle, catalogName: String, schemaName: String): Identity[OperationHandle]
}
