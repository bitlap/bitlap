/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import org.bitlap.server.rpc.{ AsyncRpcBackend, DriverGrpcServiceLive }
import zio.{ UIO, ZIO }

/** Mock live for rpc server.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */

object MockDriverGrpcServiceLive {

  val mockLive: UIO[DriverGrpcServiceLive] = ZIO.succeed(DriverGrpcServiceLive(MockAsyncRpcBackend()))

  val embedLive: UIO[DriverGrpcServiceLive] = ZIO.succeed(DriverGrpcServiceLive(AsyncRpcBackend()))

}
