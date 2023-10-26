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

import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

/** Functional RPC API, both for client and server.
 */
trait DriverX[F[_]]:
  self =>

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

  def cancelOperation(opHandle: OperationHandle): F[Unit]

  def closeOperation(opHandle: OperationHandle): F[Unit]

  def getOperationStatus(opHandle: OperationHandle): F[OperationStatus]

  def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): F[GetInfoValue]
