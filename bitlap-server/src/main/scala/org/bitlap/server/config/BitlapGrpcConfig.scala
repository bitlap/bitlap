/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.config

import org.bitlap.client.StringOpsForClient
import org.bitlap.common.BitlapConf
import org.bitlap.server.BitlapContext
import zio._

/** @author
 *    梦境迷离
 *  @version 1.0,2023/3/11
 */
final case class BitlapGrpcConfig(
  port: Int
)

object BitlapGrpcConfig {
  private val grpcPort = BitlapContext.globalConf.get(BitlapConf.NODE_BIND_HOST).extractServerAddress.port
  lazy val live: ULayer[Has[BitlapGrpcConfig]] = ZLayer.succeed(BitlapGrpcConfig(grpcPort))

}
