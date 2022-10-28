/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import scalapb.zio_grpc.{ ServerMain, ServiceList }
import zio.console.putStrLn
import zio.ZIO

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */
trait MockServer extends ServerMain {

  override def welcome: ZIO[zio.ZEnv, Throwable, Unit] =
    putStrLn(s"Mock Server is listening to port: $port")

  def services: ServiceList[zio.ZEnv] = ServiceList.addM(MockDriverGrpcServiceLive.mockLive)

}
