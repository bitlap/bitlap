/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

import zio.*

/** 函数式异步RPC API，客户端和服务端通用，基于 zio 2.0
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait DriverAsyncRpc extends DriverRpc[Task]:
  self =>

  private lazy val timeout = Duration("30s")

  override def pure[A](a: A): Task[A] = ZIO.succeed(a)

  override def map[A, B](fa: self.type => Task[A])(f: A => B): Task[B] = fa(this).map(f)

  override def flatMap[A, B](fa: self.type => Task[A])(f: A => Task[B]): Task[B] = fa(this).flatMap(f)

  def sync[T, E <: Throwable, Z <: ZIO[?, ?, ?]](
    action: self.type => Z
  ): T =
    try
      val future = Unsafe.unsafe { implicit rt =>
        Runtime.default.unsafe.runToFuture(action(this).asInstanceOf[ZIO[Any, E, T]])
      }
      Await.result(future, timeout)
    catch case e: Throwable => throw e

  def when[A, E <: Throwable](predicate: => Boolean, exception: => E, fa: self.type => Task[A]): Task[A] =
    if predicate then fa(this)
    else ZIO.fail(exception)

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

  def closeOperation(opHandle: OperationHandle): Task[Unit]

  def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus]

  def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue]
