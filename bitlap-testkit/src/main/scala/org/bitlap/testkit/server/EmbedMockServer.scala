/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import zio.{ ExitCode, URIO }
import zio.console.Console
import scalapb.zio_grpc.{ ServerMain, ServiceList }

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */
trait EmbedMockServer extends ServerMain {

  def services: ServiceList[zio.ZEnv] = ServiceList.addM(MockZioDriverServiceLive.embedLive)

}

object EmbedBitlapServer extends EmbedMockServer {

  override def port: Int                                                      = 23333
  override def run(args: List[String]): URIO[zio.ZEnv with Console, ExitCode] = super.run(args)

}
