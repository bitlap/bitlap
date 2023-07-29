/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.server.raft

import com.alipay.sofa.jraft.option.NodeOptions

/** raft配置，配置: {{ conf/bitlap.setting }}
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/10/28
 */
final case class ElectionNodeOptions(
  dataPath: String,
  // raft group id
  groupId: String,
  // ip:port
  serverAddress: String,
  // ip:port,ip:port,ip:port
  initialServerAddressList: String,
  // raft node options
  nodeOptions: NodeOptions)
