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

import scala.concurrent.duration.*
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.bitlap.common.exception.*
import org.bitlap.network.handles.SessionHandle
import org.bitlap.network.protocol.impl.{ AsyncClient, SyncClient }

import com.typesafe.scalalogging.LazyLogging

import zio.{ ULayer, ZLayer }

final class SyncConnection(user: String, password: String) extends Connection with LazyLogging {

  private var configuration: Map[String, String] = Map.empty
  private var sync: SyncClient                   = _
  private var address: ServerAddress             = _
  private var sessionId: SessionHandle           = _
  given Duration                                 = 60.seconds

  override def close(): Unit = {
    try {
      sync.closeSession(sessionId)
    } catch
      case NonFatal(e) =>
        throw BitlapSQLException("Close failed", cause = Some(e))
  }

  override def reopen(): Unit = {
    try {
      close()
      open(address, configuration)
    } catch
      case NonFatal(e) =>
        throw BitlapSQLException("Reopen failed", cause = Some(e))
  }

  override def open(address: ServerAddress, configuration: Map[String, String]): Unit = {
    try {
      if (this.sync == null) {
        this.sync = new SyncClient(List(address), configuration)
      }
      this.address = address
      this.configuration = configuration
      this.sessionId = sync.openSession(user, password, configuration)
    } catch
      case NonFatal(e) =>
        throw BitlapSQLException("Open failed", cause = Some(e))
  }

  override def open(address: ServerAddress): Unit = open(address, configuration)

  def execute(stmt: String, queryTimeout: Long = 60000, confOverlay: Map[String, String] = Map.empty)
    : BitlapSingleResult = {
    val maxTimeout = if (queryTimeout <= summon[Duration].toMillis) queryTimeout else summon[Duration].toMillis
    new BitlapSingleResult(sync, sessionId, stmt, maxTimeout, confOverlay)
  }

}
