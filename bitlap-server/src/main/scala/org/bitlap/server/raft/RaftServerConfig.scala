/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.typesafe.config.ConfigFactory
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

  private lazy val config = ConfigFactory.load().getConfig("bitlap.server.raft")

  lazy val raftServerConfig = RaftServerConfig(
    config.getString("dataPath"),
    config.getString("groupId"),
    config.getString("serverAddress"),
    config.getString("initialServerAddressList"),
    Duration.apply(config.getString("timeout"))
  )
}
