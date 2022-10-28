/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

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
  initialServerAddressList: String
)
