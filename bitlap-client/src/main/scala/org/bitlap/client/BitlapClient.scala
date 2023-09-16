/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.client

import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

/** The synchronous client used by JDBC has no logic and is all delegated to the asynchronous client
 *  [[org.bitlap.client.AsyncClient]],
 *
 *  but JDBC exclusive logic can be added to it.
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
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
