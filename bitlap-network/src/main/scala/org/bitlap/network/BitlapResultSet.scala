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

import scala.util.control.NonFatal

import org.bitlap.common.exception.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.network.protocol.impl.Sync

import com.typesafe.scalalogging.LazyLogging

final case class Result(
  tableSchema: TableSchema,
  fetchResult: FetchResults)

final class BitlapResultSet(
  sync: Sync,
  sessionId: SessionHandle,
  stmt: String,
  queryTimeout: Long,
  confOverlay: Map[String, String])
    extends Iterator[Result]
    with LazyLogging {

  private val operationId               = sync.executeStatement(sessionId, stmt, queryTimeout, confOverlay)
  private val tableSchema: TableSchema  = sync.getResultSetMetadata(operationId)
  private var fetchResult: FetchResults = _

  private var hasMore: Boolean    = false
  private var firstFetch: Boolean = true

  override def hasNext: Boolean = {
    if (firstFetch) {
      hasMore = true
      firstFetch = false
    }
    hasMore
  }

  override def next(): Result = {
    // TODO next
    if (!hasMore) throw BitlapSQLException("No more elements")
    try {
      if (fetchResult == null) {
        fetchResult = sync.fetchResults(operationId, 50, 1)
        hasMore = fetchResult.hasMoreRows
      } else {
        val operationId = sync.executeStatement(sessionId, stmt, queryTimeout, confOverlay)
        fetchResult = sync.fetchResults(operationId, 50, 1)
        hasMore = fetchResult.hasMoreRows
      }
      firstFetch = false
      Result(tableSchema, fetchResult)
    } catch
      case NonFatal(e) =>
        throw BitlapSQLException("fetch next failed", cause = Some(e))
  }
}
