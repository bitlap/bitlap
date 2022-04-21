/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import org.bitlap.server.rpc.backend.Backend
import scalapb.zio_grpc.{ServerMain, ServiceList}
import zio.console.Console
import zio.{ExitCode, URIO}

/**
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class BitlapServer(val serverPort: Int) extends ServerMain {
  override def port: Int = serverPort
  def services: ServiceList[zio.ZEnv] = ServiceList.addM(Backend.futureLive)
}
object BitlapServer extends BitlapServer(80) {
  override def run(args: List[String]): URIO[zio.ZEnv with Console, ExitCode] = super.run(args)
}
