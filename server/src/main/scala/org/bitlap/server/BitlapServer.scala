/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import org.bitlap.network.RPC
import org.bitlap.network.helper.JdbcHelper
import org.bitlap.server.rpc.processor.JdbcProcessor
import scalapb.zio_grpc.{ ServerMain, ServiceList }
import zio.console.Console
import zio.{ ExitCode, UIO, URIO, ZIO }

/**
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class BitlapServer(val serverPort: Int, jdbcHelper: JdbcHelper) extends ServerMain {
  override def port: Int = serverPort

  val live: UIO[RPC.DriverServiceLive] = ZIO.succeed(RPC.DriverServiceLive(jdbcHelper))

  def services: ServiceList[zio.ZEnv] = ServiceList.addM(live)
}
object BitlapServer extends BitlapServer(80, new JdbcProcessor()) {
  override def run(args: List[String]): URIO[zio.ZEnv with Console, ExitCode] = super.run(args)
}
