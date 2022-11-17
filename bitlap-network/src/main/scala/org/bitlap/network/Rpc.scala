/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.handles._
import org.bitlap.network.models._

/** 函数式RPC API，客户端和服务端通用。
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait Rpc[F[_]] { self =>

  def pure[A](a: A): F[A]

  def map[A, B](fa: self.type => F[A])(f: A => B): F[B]

  def flatMap[A, B](fa: self.type => F[A])(f: A => F[B]): F[B]

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

  def fetchResults(opHandle: OperationHandle, maxRows: Int, fetchType: Int): F[FetchResults]

  def getResultSetMetadata(opHandle: OperationHandle): F[TableSchema]

  def getDatabases(sessionHandle: SessionHandle, pattern: String): F[OperationHandle]

  def getTables(sessionHandle: SessionHandle, database: String, pattern: String): F[OperationHandle]

  def cancelOperation(opHandle: OperationHandle): F[Unit]

  def closeOperation(opHandle: OperationHandle): F[Unit]

  def getOperationStatus(opHandle: OperationHandle): F[OperationStatus]

}
