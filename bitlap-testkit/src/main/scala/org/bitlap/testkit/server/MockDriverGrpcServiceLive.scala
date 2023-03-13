/* Copyright (c) 2023 bitlap.org */
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
  val mockLive: ZLayer[Any, Nothing, Has[GrpcServiceLive]] = MockAsyncRpcBackend.live >>> GrpcServiceLive.live

  /** 真实数据
   */
  val embedLive: ZLayer[Any, Nothing, Has[GrpcServiceLive]] =
    GrpcBackendLive.live >>> GrpcServiceLive.live

}
