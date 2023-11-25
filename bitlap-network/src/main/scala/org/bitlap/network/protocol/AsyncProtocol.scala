/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org
package bitlap
package network
package protocol

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.control.NonFatal

import org.bitlap.common.exception.BitlapException
import org.bitlap.common.exception.BitlapIllegalStateException
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

import zio.{ Runtime, Task, UIO, Unsafe, ZIO }

/** Functional asynchronous RPC API, both for client and server.
 */
trait AsyncProtocol extends ProtocolMonad[Task]:
  self =>

  private lazy val defaultStateException = new BitlapIllegalStateException(
    "Invalid node state, this operation must be on the master node"
  )

  override def pure[A](a: A): Task[A] = ZIO.succeed(a)

  override def map[A, B](fa: PMonad => Task[A])(f: A => B): Task[B] = fa(this).map(f)

  override def flatMap[A, B](fa: PMonad => Task[A])(f: A => Task[B]): Task[B] = fa(this).flatMap(f)

  def sync[T](action: PMonad => Task[T])(using timeout: Duration): T =
    try {
      val future = Unsafe.unsafe { implicit rt =>
        Runtime.default.unsafe.runToFuture(action(this))
      }
      Await.result(future, timeout)
    } catch
      case NonFatal(e) =>
        throw BitlapException(s"${e.getLocalizedMessage}", cause = Option(e))

  def when[A, E <: Throwable](
    predicate: => ZIO[Any, Throwable, Boolean],
    fa: PMonad => Task[A],
    exception: => E = defaultStateException
  ): Task[A] =
    ZIO
      .whenZIO(predicate)
      .apply {
        fa(this)
      }
      .someOrFail(exception)

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

  def cancelOperation(opHandle: OperationHandle): Task[Unit]

  def closeOperation(opHandle: OperationHandle): Task[Unit]

  def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus]

  def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue]
