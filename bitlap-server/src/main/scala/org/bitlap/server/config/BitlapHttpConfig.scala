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
final case class BitlapHttpConfig(
  port: Int
)

object BitlapHttpConfig {
  private val httpPort = BitlapContext.globalConf.get(BitlapConf.HTTP_SERVER_ADDRESS).asServerAddress.port
  lazy val live: ULayer[Has[BitlapHttpConfig]] = ZLayer.succeed(BitlapHttpConfig(httpPort))

}
