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
package org.bitlap.network

import org.bitlap.network.handles.*
import org.bitlap.network.models.*

/** Functional synchronous RPC API, both for client and server, logic should be delegated to asynchronous RPC
 *  [[org.bitlap.network.AsyncProtocol]] and should not be implemented on its own.
 */
trait SyncProtocol extends ProtocolMonad[Identity]:
  self =>

  def pure[A](a: A): Identity[A] = a

  def map[A, B](fa: Monad => Identity[A])(f: A => B): Identity[B] = f(fa(this))

  def flatMap[A, B](fa: Monad => Identity[A])(f: A => Identity[B]): Identity[B] = f(fa(this))

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

  def cancelOperation(opHandle: OperationHandle): Identity[Unit]

  def getOperationStatus(opHandle: OperationHandle): Identity[OperationStatus]
