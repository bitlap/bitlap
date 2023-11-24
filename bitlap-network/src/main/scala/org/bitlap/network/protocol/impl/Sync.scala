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
package org.bitlap.network.protocol.impl

import org.bitlap.network.*
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.network.protocol.SyncProtocol

/** Synchronous RPC clients have no logic and are all delegated to asynchronous clients
 *  [[org.bitlap.network.protocol.impl.Async]].
 */
final class Sync(serverPeers: List[ServerAddress], props: Map[String, String]) extends SyncProtocol:

  private val async = new Async(serverPeers, props)

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Identity[SessionHandle] = async.sync {
    _.openSession(username, password, configuration)
  }

  override def closeSession(sessionHandle: SessionHandle): Identity[Unit] = async.sync {
    _.closeSession(sessionHandle)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): Identity[OperationHandle] = async.sync {
    _.executeStatement(sessionHandle, statement, queryTimeout, confOverlay)
  }

  override def fetchResults(opHandle: OperationHandle, maxRows: Int, fetchType: Int): Identity[FetchResults] =
    async.sync {
      _.fetchResults(opHandle, maxRows, fetchType)
    }

  override def getResultSetMetadata(opHandle: OperationHandle): Identity[TableSchema] = async.sync {
    _.getResultSetMetadata(opHandle)
  }

  override def cancelOperation(opHandle: OperationHandle): Identity[Unit] =
    async.sync {
      _.cancelOperation(opHandle)
    }

  override def getOperationStatus(opHandle: OperationHandle): Identity[OperationStatus] =
    async.sync {
      _.getOperationStatus(opHandle)
    }

  override def closeOperation(opHandle: OperationHandle): Identity[Unit] =
    async.sync {
      _.closeOperation(opHandle)
    }

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Identity[GetInfoValue] = ???
