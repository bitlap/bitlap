/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.handles._
import org.bitlap.network.models._
import zio._

/** 函数式异步RPC API，客户端和服务端通用，基于 zio 1.0
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait AsyncRpc extends Rpc[Task] { self =>

  override def pure[A](a: A): Task[A] = Task.succeed(a)

  override def map[A, B](fa: self.type => Task[A])(f: A => B): Task[B] = fa(this).map(f)

  override def flatMap[A, B](fa: self.type => Task[A])(f: A => Task[B]): Task[B] = fa(this).flatMap(f)

  def sync[T, Z <: ZIO[_, _, _]](action: self.type => Z)(implicit runtime: zio.Runtime[Any] = zio.Runtime.default): T =
    runtime.unsafeRun(action(this).asInstanceOf[ZIO[Any, Throwable, T]])

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

  def getDatabases(sessionHandle: SessionHandle, pattern: String): Task[OperationHandle]

  def getTables(sessionHandle: SessionHandle, database: String, pattern: String): Task[OperationHandle]

  def cancelOperation(opHandle: OperationHandle): Task[Unit]

  def getOperationStatus(opHandle: OperationHandle): Task[OperationState]

}
