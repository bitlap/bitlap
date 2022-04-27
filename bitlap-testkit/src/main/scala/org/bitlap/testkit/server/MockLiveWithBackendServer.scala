/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import scalapb.zio_grpc.{ ServerMain, ServiceList }

trait MockLiveWithBackendServer extends ServerMain {

  def services: ServiceList[zio.ZEnv] = ServiceList.addM(MockZioDriverServiceLive.mockLiveWithBackend)

}
