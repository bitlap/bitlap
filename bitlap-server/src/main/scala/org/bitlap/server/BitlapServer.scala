/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import org.bitlap.server.rpc.live.Live
import scalapb.zio_grpc.{ ServerMain, ServiceList }
import zio.console.Console
import zio.{ ExitCode, URIO }

/**
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class BitlapServer(val serverPort: Int) extends ServerMain {

  override def port: Int = serverPort

  def services: ServiceList[zio.ZEnv] = ServiceList.addM(Live.zioLive) // 可以随意更换实现

}

object Server extends BitlapServer(23333) {

  override def run(args: List[String]): URIO[zio.ZEnv with Console, ExitCode] = super.run(args)

}
