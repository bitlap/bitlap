/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network

import org.bitlap.network.NetworkException.RpcException
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles._
import org.bitlap.network.models._
import zio._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/** 函数式异步RPC API，客户端和服务端通用，基于 zio 1.0
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait DriverAsyncRpc extends DriverRpc[Task] { self =>

  private lazy val runtime = zio.Runtime.default
  private lazy val timeout = Duration("30s")

  override def pure[A](a: A): Task[A] = Task.succeed(a)

  override def map[A, B](fa: self.type => Task[A])(f: A => B): Task[B] = fa(this).map(f)

  override def flatMap[A, B](fa: self.type => Task[A])(f: A => Task[B]): Task[B] = fa(this).flatMap(f)

  def sync[T, E <: Throwable, Z <: ZIO[_, _, _]](
    action: self.type => Z
  )(implicit runtime: zio.Runtime[Any] = runtime): T =
    try {
      val cancelableFuture = runtime.unsafeRunToFuture(action(this).asInstanceOf[ZIO[Any, E, T]])
      Await.result(cancelableFuture.future, timeout)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw RpcException(msg = "Sync future exception", cause = Option(e))
        null.asInstanceOf[T]
    }

  def when[A, E <: Throwable](predicate: => Boolean, exception: => E, fa: self.type => Task[A]): Task[A] =
    if (predicate) {
      fa(this)
    } else { ZIO.fail(exception) }

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

}
