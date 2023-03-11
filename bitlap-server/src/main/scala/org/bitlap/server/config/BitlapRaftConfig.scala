/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.config

import org.bitlap.common.BitlapConf
import org.bitlap.server.BitlapServerContext
import zio._

import scala.concurrent.duration.Duration

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/28
 */
final case class BitlapRaftConfig(
  dataPath: String,
  // raft group id
  groupId: String,
  // ip:port
  serverAddress: String,
  // ip:port,ip:port,ip:port
  initialServerAddressList: String,
  // 5s 100ms
  timeout: Duration
)
object BitlapRaftConfig {

  lazy val live: ULayer[Has[BitlapRaftConfig]] = ZLayer.succeed(
    BitlapRaftConfig(
      BitlapServerContext.globalConf.get(BitlapConf.RAFT_DATA_PATH),
      BitlapServerContext.globalConf.get(BitlapConf.NODE_GROUP_ID),
      BitlapServerContext.globalConf.get(BitlapConf.RAFT_SERVER_ADDRESS),
      BitlapServerContext.globalConf.get(BitlapConf.RAFT_INITIAL_SERVER_ADDRESS),
      Duration(BitlapServerContext.globalConf.get(BitlapConf.RAFT_TIMEOUT))
    )
  )
}
