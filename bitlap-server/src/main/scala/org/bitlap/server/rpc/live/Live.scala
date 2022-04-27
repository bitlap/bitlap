/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.live

import org.bitlap.server.rpc.backend.{ FutureRpcBackend, SyncRpcBackend, ZioRpcBackend }
import zio.{ UIO, ZIO }

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/22
 */
object Live {

  lazy val zioRpcBackend: ZioRpcBackend = ZioRpcBackend()

  lazy val zioLive: UIO[ZioDriverServiceLive] = ZIO.succeed(ZioDriverServiceLive(ZioRpcBackend()))

  lazy val futureLive: UIO[FutureDriverServiceLive] =
    ZIO.succeed(FutureDriverServiceLive(FutureRpcBackend(zioRpcBackend)))

  lazy val syncLive: UIO[SyncDriverServiceLive] = ZIO.succeed(SyncDriverServiceLive(SyncRpcBackend(zioRpcBackend)))

}
