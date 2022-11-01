/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import org.bitlap.server.rpc._
import zio._

/** 用于测试的 bitlap rpc 服务端API实现
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */

object MockDriverGrpcServiceLive {

  /** 固定的测试数据
   */
  val mockLive: UIO[GrpcServiceLive] = ZIO.succeed(GrpcServiceLive(MockAsyncRpcBackend()))

  /** 真实数据
   */
  val embedLive: UIO[GrpcServiceLive] = ZIO.succeed(GrpcServiceLive(AsyncRpcBackend()))

}
