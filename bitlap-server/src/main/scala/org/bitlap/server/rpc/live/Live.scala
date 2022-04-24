/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.live

import zio.{ UIO, ZIO }

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/22
 */
object Live {

  lazy val futureLive: UIO[FutureDriverServiceLive] = ZIO.succeed(FutureDriverServiceLive())

  lazy val syncLive: UIO[SyncDriverServiceLive] = ZIO.succeed(SyncDriverServiceLive())

  lazy val zioLive: UIO[ZioDriverServiceLive] = ZIO.succeed(ZioDriverServiceLive())

}
