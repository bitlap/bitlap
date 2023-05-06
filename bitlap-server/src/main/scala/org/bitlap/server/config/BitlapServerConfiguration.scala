/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.config

import zio.ZLayer
import org.bitlap.common.BitlapConf
import org.bitlap.client.*
import zio.*
import scala.concurrent.duration.Duration
import org.bitlap.server.BitlapContext
import org.bitlap.network.ServerAddress

/** 包装kotlin配置，组成统一的Layer暴露出去
 *  @author
 *    梦境迷离
 *  @version 1.0,2023/5/6
 */
object BitlapServerConfiguration:

  private[server] lazy val config = new BitlapConf()

  lazy val live: ZLayer[Any, Nothing, BitlapServerConfiguration] = ZLayer.make[BitlapServerConfiguration](
    ZLayer.succeed(config),
    ZLayer.fromFunction((underlayConf: BitlapConf) => BitlapServerConfiguration(underlayConf))
  )
  lazy val testLive: ZLayer[Any, Nothing, BitlapServerConfiguration] = live

end BitlapServerConfiguration

final case class BitlapServerConfiguration(underlayConf: BitlapConf):

  val grpcConfig: BitlapGrpcConfig = BitlapGrpcConfig(
    underlayConf.get(BitlapConf.NODE_BIND_HOST).asServerAddress.port
  )

  val raftConfig: BitlapRaftConfig = BitlapRaftConfig(
    underlayConf.get(BitlapConf.RAFT_DATA_PATH),
    underlayConf.get(BitlapConf.NODE_GROUP_ID),
    underlayConf.get(BitlapConf.RAFT_SERVER_ADDRESS),
    underlayConf.get(BitlapConf.RAFT_INITIAL_SERVER_ADDRESS),
    Duration(underlayConf.get(BitlapConf.RAFT_TIMEOUT))
  )

  val httpConfig: BitlapHttpConfig =
    BitlapHttpConfig(
      underlayConf.get(BitlapConf.HTTP_SERVER_ADDRESS).asServerAddress.port,
      underlayConf.get[Integer](BitlapConf.HTTP_THREADS)
    )

  val sessionConfig: BitlapSessionConfig = BitlapSessionConfig(
    Duration(underlayConf.get(BitlapConf.SESSION_TIMEOUT)),
    Duration(underlayConf.get(BitlapConf.SESSION_INTERVAL))
  )

end BitlapServerConfiguration
