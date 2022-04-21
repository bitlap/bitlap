package org.bitlap.server.rpc.backend

import org.bitlap.network.RPC
import zio.{ UIO, ZIO }

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
object Backend {

  lazy val futureBackend = new JdbcFutureBackend()
  val futureLive: UIO[RPC.FutureDriverServiceLive] = ZIO.succeed(RPC.FutureDriverServiceLive(futureBackend))

  lazy val syncBackend = new JdbcSyncBackend()
  val syncLive: UIO[RPC.SyncDriverServiceLive] = ZIO.succeed(RPC.SyncDriverServiceLive(syncBackend))

}
