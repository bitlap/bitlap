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

import org.bitlap.common.exception._
import org.bitlap.network.handles.SessionHandle
import org.bitlap.network.protocol.impl.Sync

import com.typesafe.scalalogging.LazyLogging

final class SyncConnection(user: String, password: String) extends Connection with LazyLogging {

  private var configuration: Map[String, String] = Map.empty
  private var sync: Sync                         = _
  private var address: ServerAddress             = _
  private var sessionId: SessionHandle           = _

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
      this.sync = new Sync(List(address), configuration)
      this.address = address
      this.configuration = configuration
      this.sessionId = sync.openSession(user, password, configuration)
    } catch
      case NonFatal(e) =>
        throw BitlapSQLException("Open failed", cause = Some(e))
  }

  override def open(address: ServerAddress): Unit = open(address, configuration)

  def execute(stmt: String, queryTimeout: Long = 600000, confOverlay: Map[String, String] = Map.empty)
    : BitlapResultSet = {
    new BitlapResultSet(sync, sessionId, stmt, queryTimeout, confOverlay)
  }

}
