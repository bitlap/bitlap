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

import java.util.concurrent.atomic.AtomicBoolean

import org.bitlap.common.BitlapConf
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

import zio.Task

/** Bitlap session
 */
trait Session {

  val sessionHandle: SessionHandle
  val password: String
  val username: String
  val sessionManager: SessionManager

  def lastAccessTime: Long

  def lastAccessTime_=(time: Long): Unit

  def sessionConf: BitlapConf

  def sessionState: AtomicBoolean

  def creationTime: Long

  def currentSchema: String

  def currentSchema_=(schema: String): Unit

  def open(): Unit

  def executeStatement(
    statement: String,
    confOverlay: Map[String, String]
  ): OperationHandle

  def executeStatement(
    statement: String,
    confOverlay: Map[String, String] = Map.empty,
    queryTimeout: Long
  ): OperationHandle

  def fetchResults(operationHandle: OperationHandle): Task[RowSet]

  def getResultSetMetadata(operationHandle: OperationHandle): Task[TableSchema]

  def closeOperation(operationHandle: OperationHandle): Unit

  def cancelOperation(operationHandle: OperationHandle): Unit

  def removeExpiredOperations(handles: List[OperationHandle]): List[Operation]

  def getNoOperationTime: Long

  def getInfo(getInfoType: GetInfoType): Task[GetInfoValue]
}
