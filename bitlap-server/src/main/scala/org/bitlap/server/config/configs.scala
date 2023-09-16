/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.server.config

import scala.concurrent.duration.Duration

/** @author
 *    梦境迷离
 *  @version 1.0,2023/5/6
 */
/** gRPC Configurations
 */
private[config] final case class BitlapGrpcConfig(port: Int)

/** HTTP Configurations
 */
private[config] final case class BitlapHttpConfig(port: Int, threads: Int)

/** Session Configurations
 */
private[config] final case class BitlapSessionConfig(timeout: Duration, interval: Duration)

/** Raft Configurations
 */
private[config] final case class BitlapRaftConfig(
  dataPath: String,
  // raft group id
  groupId: String,
  // ip:port
  serverAddress: String,
  // ip:port,ip:port,ip:port
  initialServerAddressList: String,
  // 5s 100ms
  timeout: Duration)
