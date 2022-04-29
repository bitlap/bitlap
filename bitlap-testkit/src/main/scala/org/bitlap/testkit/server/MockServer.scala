/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import scalapb.zio_grpc.{ ServerMain, ServiceList }

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/27
 */
trait MockServer extends ServerMain {

  def services: ServiceList[zio.ZEnv] = ServiceList.addM(MockZioDriverServiceLive.mockLive)

}
