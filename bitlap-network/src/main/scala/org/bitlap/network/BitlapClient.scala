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
import org.bitlap.network.protocol.impl.Sync

/** The synchronization client used by JDBC is delegated to [[org.bitlap.network.protocol.impl.Async]]
 */
final class BitlapClient(serverPeers: List[ServerAddress], props: Map[String, String]):

  private lazy val sync: Sync = new Sync(serverPeers, props)

  def openSession(
    username: String = "",
    password: String = "",
    config: Map[String, String] = Map.empty
  ): SessionHandle =
    sync.openSession(username, password, config)

  def closeSession(sessionHandle: SessionHandle): Unit =
    sync.closeSession(sessionHandle)

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    config: Map[String, String] = Map.empty
  ): OperationHandle =
    sync.executeStatement(sessionHandle, statement, queryTimeout, config)

  def fetchResults(operationHandle: OperationHandle, maxRows: Int, fetchType: Int): FetchResults =
    sync.fetchResults(operationHandle, maxRows, fetchType)

  def getResultSetMetadata(operationHandle: OperationHandle): TableSchema =
    sync.getResultSetMetadata(operationHandle)

  def cancelOperation(opHandle: OperationHandle): Unit =
    sync.cancelOperation(opHandle)

  def closeOperation(opHandle: OperationHandle): Unit =
    sync.closeOperation(opHandle)

  def getOperationStatus(opHandle: OperationHandle): OperationStatus =
    sync.getOperationStatus(opHandle)

  def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue =
    sync.getInfo(sessionHandle, getInfoType)

  def authenticate(username: String, password: String) = sync.authenticate(username, password)
