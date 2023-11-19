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
package org.bitlap.server.session

import java.util.concurrent.atomic.{ AtomicBoolean, AtomicLong, AtomicReference }

import scala.collection.mutable

import org.bitlap.common.BitlapConf
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

import zio.{ Ref, Task }

/** Bitlap session
 */
trait Session {

  val sessionHandle: SessionHandle
  val password: String
  val username: String
  val sessionManager: SessionManager

  def lastAccessTimeRef: Ref[AtomicLong]

  def sessionConfRef: Ref[mutable.Map[String, String]]

  def sessionStateRef: Ref[AtomicBoolean]

  def creationTimeRef: Ref[AtomicLong]

  def currentSchemaRef: Ref[AtomicReference[String]]

  def executeStatement(
    statement: String,
    confOverlay: Map[String, String]
  ): Task[OperationHandle]

  def executeStatement(
    statement: String,
    confOverlay: Map[String, String] = Map.empty,
    queryTimeout: Long
  ): Task[OperationHandle]

  def fetchResults(operationHandle: OperationHandle): Task[RowSet]

  def getResultSetMetadata(operationHandle: OperationHandle): Task[TableSchema]

  def closeOperation(operationHandle: OperationHandle): Task[Unit]

  def cancelOperation(operationHandle: OperationHandle): Task[Unit]

  def removeExpiredOperations(handles: List[OperationHandle]): Task[List[Operation]]

  def getNoOperationTime: Task[Long]

  def getInfo(getInfoType: GetInfoType): Task[GetInfoValue]
}
