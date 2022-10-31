/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import org.bitlap.common.BitlapConf

import scala.concurrent.duration.Duration

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/28
 */
final case class RaftServerConfig(
  dataPath: String,
  // raft group id
  groupId: String,
  // ip:port
  serverAddress: String,
  // ip:port,ip:port,ip:port
  initialServerAddressList: String,
  timeout: Duration
)
object RaftServerConfig {

  private lazy val conf = new BitlapConf()

  lazy val raftServerConfig: RaftServerConfig = RaftServerConfig(
    conf.get(BitlapConf.RAFT_DATA_PATH),
    conf.get(BitlapConf.RAFT_GROUP_ID),
    conf.get(BitlapConf.RAFT_SERVER_ADDRESS),
    conf.get(BitlapConf.RAFT_INITIAL_SERVER_ADDRESS_LIST),
    Duration.apply(conf.get(BitlapConf.RAFT_TIMEOUT))
  )
}
