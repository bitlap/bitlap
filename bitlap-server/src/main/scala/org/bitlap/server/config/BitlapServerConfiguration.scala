/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.config

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.bitlap.client.*
import org.bitlap.common.{ BitlapConf, BitlapConfs }
import org.bitlap.network.ServerAddress
import org.bitlap.server.BitlapContext

import zio.*

/** 包装kotlin配置，组成统一的Layer暴露出去
 *  @author
 *    梦境迷离
 *  @version 1.0,2023/5/6
 */
object BitlapServerConfiguration:

  lazy val live: ZLayer[Any, Nothing, BitlapServerConfiguration] = ZLayer.make[BitlapServerConfiguration](
    ZLayer.succeed(BitlapContext.globalConf),
    ZLayer.fromFunction((underlayConf: BitlapConf) => BitlapServerConfiguration(underlayConf))
  )
  lazy val testLive: ZLayer[Any, Nothing, BitlapServerConfiguration] = live

end BitlapServerConfiguration

final case class BitlapServerConfiguration(underlayConf: BitlapConf):

  val grpcConfig: BitlapGrpcConfig = BitlapGrpcConfig(
    underlayConf.get(BitlapConfs.NODE_HOST).asServerAddress.port
  )

  val raftConfig: BitlapRaftConfig = BitlapRaftConfig(
    underlayConf.get(BitlapConfs.NODE_RAFT_DIR),
    underlayConf.get(BitlapConfs.NODE_RAFT_GROUP_ID),
    underlayConf.get(BitlapConfs.NODE_RAFT_HOST),
    underlayConf.get(BitlapConfs.NODE_RAFT_PEERS),
    Duration(underlayConf.getMillis(BitlapConfs.NODE_RAFT_TIMEOUT), TimeUnit.MILLISECONDS)
  )

  val httpConfig: BitlapHttpConfig =
    BitlapHttpConfig(
      underlayConf.get(BitlapConfs.NODE_HTTP_HOST).asServerAddress.port,
      underlayConf.get[Integer](BitlapConfs.NODE_HTTP_THREADS)
    )

  val sessionConfig: BitlapSessionConfig = BitlapSessionConfig(
    Duration(underlayConf.getMillis(BitlapConfs.NODE_SESSION_EXPIRY_PERIOD), TimeUnit.MILLISECONDS),
    Duration(underlayConf.getMillis(BitlapConfs.NODE_SESSION_EXPIRY_INTERVAL), TimeUnit.MILLISECONDS)
  )

end BitlapServerConfiguration
