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

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.bitlap.client.*
import org.bitlap.common.BitlapConf
import org.bitlap.common.conf.BitlapConfKeys
import org.bitlap.network.ServerAddress

import zio.*

/** Wrapping kotlin configuration, forming a unified layer exposed.
 */
object BitlapServerConfiguration:

  lazy val live: ZLayer[Any, Nothing, BitlapServerConfiguration] = ZLayer.make[BitlapServerConfiguration](
    ZLayer.succeed(org.bitlap.core.BitlapContext.bitlapConf),
    ZLayer.fromFunction((underlayConf: BitlapConf) => BitlapServerConfiguration(underlayConf))
  )
  lazy val testLive: ZLayer[Any, Nothing, BitlapServerConfiguration] = live

end BitlapServerConfiguration

final case class BitlapServerConfiguration(underlayConf: BitlapConf):

  val grpcConfig: BitlapGrpcConfig = BitlapGrpcConfig(
    underlayConf.get(BitlapConfKeys.NODE_HOST).asServerAddress.port
  )

  val raftConfig: BitlapRaftConfig = BitlapRaftConfig(
    underlayConf.get(BitlapConfKeys.NODE_RAFT_DIR),
    underlayConf.get(BitlapConfKeys.NODE_RAFT_GROUP_ID),
    underlayConf.get(BitlapConfKeys.NODE_RAFT_HOST),
    underlayConf.get(BitlapConfKeys.NODE_RAFT_PEERS),
    Duration(underlayConf.getMillis(BitlapConfKeys.NODE_RAFT_TIMEOUT), TimeUnit.MILLISECONDS)
  )

  val httpConfig: BitlapHttpConfig =
    BitlapHttpConfig(
      underlayConf.get(BitlapConfKeys.NODE_HTTP_HOST).asServerAddress.port,
      underlayConf.get[Integer](BitlapConfKeys.NODE_HTTP_THREADS)
    )

  val sessionConfig: BitlapSessionConfig = BitlapSessionConfig(
    Duration(underlayConf.getMillis(BitlapConfKeys.NODE_SESSION_EXPIRY_PERIOD), TimeUnit.MILLISECONDS),
    Duration(underlayConf.getMillis(BitlapConfKeys.NODE_SESSION_EXPIRY_INTERVAL), TimeUnit.MILLISECONDS)
  )

end BitlapServerConfiguration
