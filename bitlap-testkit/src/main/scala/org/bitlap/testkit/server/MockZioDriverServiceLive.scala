/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import org.bitlap.server.rpc.{ ZioDriverServiceLive, ZioRpcBackend }
import zio.{ UIO, ZIO }

/** Mock live for rpc server.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */

object MockZioDriverServiceLive {

  val mockLive: UIO[ZioDriverServiceLive] = ZIO.succeed(ZioDriverServiceLive(MockZioRpcBackend()))

  val embedLive: UIO[ZioDriverServiceLive] = ZIO.succeed(ZioDriverServiceLive(ZioRpcBackend()))

}
