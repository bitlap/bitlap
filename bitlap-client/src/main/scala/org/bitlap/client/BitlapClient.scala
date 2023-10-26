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
package org.bitlap.client

import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

/** The synchronous client used by JDBC has no logic and is all delegated to the asynchronous client
 *  [[org.bitlap.client.AsyncClient]],
 *
 *  but JDBC exclusive logic can be added to it.
 */
final class BitlapClient(serverPeers: Array[String], props: Map[String, String]):

  private lazy val syncClient: SyncClient = new SyncClient(serverPeers, props)

  def openSession(
    username: String = "",
    password: String = "",
    config: Map[String, String] = Map.empty
  ): SessionHandle =
    syncClient.openSession(username, password, config)

  def closeSession(sessionHandle: SessionHandle): Unit =
    syncClient.closeSession(sessionHandle)

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    config: Map[String, String] = Map.empty
  ): OperationHandle =
    syncClient.executeStatement(sessionHandle, statement, queryTimeout, config)

  def fetchResults(operationHandle: OperationHandle, maxRows: Int, fetchType: Int): FetchResults =
    syncClient.fetchResults(operationHandle, maxRows, fetchType)

  def getResultSetMetadata(operationHandle: OperationHandle): TableSchema =
    syncClient.getResultSetMetadata(operationHandle)

  def cancelOperation(opHandle: OperationHandle): Unit =
    syncClient.cancelOperation(opHandle)

  def closeOperation(opHandle: OperationHandle): Unit =
    syncClient.closeOperation(opHandle)

  def getOperationStatus(opHandle: OperationHandle): OperationStatus =
    syncClient.getOperationStatus(opHandle)

  def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue =
    syncClient.getInfo(sessionHandle, getInfoType)
