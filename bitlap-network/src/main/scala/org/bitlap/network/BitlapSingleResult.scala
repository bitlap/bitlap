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

import scala.collection.View
import scala.util.control.NonFatal

import org.bitlap.common.exception.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.network.protocol.impl.SyncClient

import com.typesafe.scalalogging.LazyLogging

final class BitlapSingleResult(
  sync: SyncClient,
  sessionId: SessionHandle,
  stmt: String,
  queryTimeout: Long,
  confOverlay: Map[String, String])
    extends Iterable[Result]
    with LazyLogging {

  private lazy val operationId: OperationHandle = {
    try {
      if (sessionId != null) {
        sync.executeStatement(sessionId, stmt, queryTimeout, confOverlay)
      } else {
        throw BitlapRuntimeException(s"Invalid sessionId: $sessionId")
      }
    } catch
      case NonFatal(e) =>
        throw BitlapSQLException("Fetch metadata failed", cause = Some(e))
  }

  private lazy val tableSchema: TableSchema = {
    try {
      if (operationId != null) {
        sync.getResultSetMetadata(operationId)
      } else {
        throw BitlapRuntimeException(s"Invalid operationId: $operationId")
      }
    } catch
      case NonFatal(e) =>
        throw BitlapSQLException("Fetch metadata failed", cause = Some(e))

  }
  private lazy val fetchResult: FetchResults = sync.fetchResults(operationId, 1000, 1)
  private lazy val result: Result            = Result(tableSchema, fetchResult)

  override def iterator: Iterator[Result] = Iterator.single(result)

  override def knownSize: Int = 1

  override def head: Result = result

  override def headOption: Option[Result] = Some(result)

  override def last: Result = result

  override def lastOption: Option[Result] = Some(result)

  override def view: View.Single[Result] = new View.Single(result)

  override def take(n: Int): Iterable[Result] = if (n > 0) this else Iterable.empty

  override def takeRight(n: Int): Iterable[Result] = if (n > 0) this else Iterable.empty

  override def drop(n: Int): Iterable[Result] = if (n > 0) Iterable.empty else this

  override def dropRight(n: Int): Iterable[Result] = if (n > 0) Iterable.empty else this

  override def tail: Iterable[Result] = Iterable.empty

  override def init: Iterable[Result] = Iterable.empty
}
