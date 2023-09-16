/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.client

import org.bitlap.network.*
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

/** Synchronous RPC clients have no logic and are all delegated to asynchronous clients
 *  [[org.bitlap.client.AsyncClient]].
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
 */
final class SyncClient(serverPeers: Array[String], props: Map[String, String]) extends DriverIdentity:

  private lazy val delegateClient = new AsyncClient(serverPeers, props)

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Identity[SessionHandle] = delegateClient.sync {
    _.openSession(username, password, configuration)
  }

  override def closeSession(sessionHandle: SessionHandle): Identity[Unit] = delegateClient.sync {
    _.closeSession(sessionHandle)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): Identity[OperationHandle] = delegateClient.sync {
    _.executeStatement(sessionHandle, statement, queryTimeout, confOverlay)
  }

  override def fetchResults(opHandle: OperationHandle, maxRows: Int, fetchType: Int): Identity[FetchResults] =
    delegateClient.sync {
      _.fetchResults(opHandle, maxRows, fetchType)
    }

  override def getResultSetMetadata(opHandle: OperationHandle): Identity[TableSchema] = delegateClient.sync {
    _.getResultSetMetadata(opHandle)
  }

  def getLeader(requestId: String): Identity[ServerAddress] = delegateClient.sync {
    _.getLeader(requestId)
  }

  override def cancelOperation(opHandle: OperationHandle): Identity[Unit] =
    delegateClient.sync {
      _.cancelOperation(opHandle)
    }

  override def getOperationStatus(opHandle: OperationHandle): Identity[OperationStatus] =
    delegateClient.sync {
      _.getOperationStatus(opHandle)
    }

  override def closeOperation(opHandle: OperationHandle): Identity[Unit] =
    delegateClient.sync {
      _.closeOperation(opHandle)
    }

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Identity[GetInfoValue] = ???
