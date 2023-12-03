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
package org.bitlap.server.config

import scala.concurrent.duration.Duration

/** gRPC Configurations
 */
private[config] final case class BitlapGrpcConfig(host: String, port: Int, clientPeers: String)

/** HTTP Configurations
 */
private[config] final case class BitlapHttpConfig(port: Int, threads: Int)

/** Session Configurations
 */
private[config] final case class BitlapSessionConfig(timeout: Duration, interval: Duration, sql: Duration)

/** Raft Configurations
 */
private[config] final case class BitlapRaftConfig(
  dataPath: String,
  // raft group id
  groupId: String,
  // ip:port
  serverAddress: String,
  // ip:port,ip:port,ip:port
  initialServerAddressList: String,
  // 5s 100ms
  timeout: Duration) {

  def getPorts: List[Int] = {
    val addrs = initialServerAddressList.split(",").toList.filter(_.contains(":"))
    addrs.map(_.split(":")(1)).toList.map(_.toInt)
  }
}
