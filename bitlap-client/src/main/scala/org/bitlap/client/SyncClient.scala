/* Copyright (c) 2022 bitlap.org */
package org.bitlap.client

import org.bitlap.network._
import org.bitlap.network.handles._
import org.bitlap.network.models._

/** 同步的RPC客户端，本身无逻辑，全部都委托给异步客户端。
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
 */
class SyncClient(serverPeers: Array[String], props: Map[String, String]) extends SyncRpc {

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

  override def getDatabases(sessionHandle: SessionHandle, pattern: String): Identity[OperationHandle] =
    delegateClient.sync {
      _.getDatabases(sessionHandle, pattern)
    }
  override def getTables(sessionHandle: SessionHandle, database: String, pattern: String): Identity[OperationHandle] =
    delegateClient.sync {
      _.getTables(sessionHandle, database, pattern)
    }

  def getLeader(requestId: String): Identity[ServerAddress] = delegateClient.sync {
    _.getLeader(requestId)
  }

  override def cancelOperation(opHandle: OperationHandle): Identity[Unit] =
    delegateClient.sync {
      _.cancelOperation(opHandle)
    }

  override def getOperationStatus(opHandle: OperationHandle): Identity[OperationState] =
    delegateClient.sync {
      _.getOperationStatus(opHandle)
    }
}
