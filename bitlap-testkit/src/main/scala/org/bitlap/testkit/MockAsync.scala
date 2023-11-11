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
package org.bitlap.testkit

import org.bitlap.network.*
import org.bitlap.network.enumeration.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.network.protocol.AsyncProtocol
import org.bitlap.network.serde.BitlapSerde
import org.bitlap.testkit.*

import com.google.protobuf.ByteString

import zio.*

/** Bitlap RPC server implementation for testing
 *
 *  data: simple_data.csv
 */
object MockAsync {
  lazy val live: ULayer[AsyncProtocol] = ZLayer.succeed(new MockAsync())
}

final class MockAsync extends AsyncProtocol with CSVUtils {

  val metrics: Seq[Metric] = readClasspathCSVData("simple_data.csv")

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, SessionHandle] = ZIO.succeed(SessionHandle(HandleIdentifier()))

  override def closeSession(sessionHandle: SessionHandle): ZIO[Any, Throwable, Unit] = ZIO.unit

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, OperationHandle] =
    ZIO.succeed(OperationHandle(OperationType.ExecuteStatement, true, sessionHandle.handleId))

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int,
    fetchType: Int
  ): ZIO[Any, Throwable, FetchResults] = {
    val convert = (metric: Metric) =>
      List(
        BitlapSerde.serialize(metric.time),
        BitlapSerde.serialize(metric.entity),
        BitlapSerde.serialize(metric.dimensions.map(_.value).headOption.getOrElse("")),
        BitlapSerde.serialize(metric.name),
        BitlapSerde.serialize(metric.value)
      )
    ZIO.succeed(
      FetchResults(
        hasMoreRows = false,
        RowSet(
          metrics.toList.map(m => Row(convert(m)))
        )
      )
    )
  }

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    ZIO.succeed(
      TableSchema.apply(
        List(
          ColumnDesc("time", TypeId.LongType),
          ColumnDesc("entity", TypeId.IntType),
          ColumnDesc("dimensions", TypeId.StringType), // TODO not support object type
          ColumnDesc("metric_name", TypeId.StringType),
          ColumnDesc("metric_value", TypeId.IntType)
        )
      )
    )

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] = ZIO.unit

  override def closeOperation(opHandle: OperationHandle): Task[Unit] = ZIO.unit

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    ZIO.succeed(OperationStatus(Some(true), Some(OperationState.FinishedState)))

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] = ???
}
