/* Copyright (c) 2022 bitlap.org */
package org.bitlap.it

import org.bitlap.testkit.server.EmbedMockServer
import zio.{ ExitCode, URIO }
import zio.console.Console

object EmbedBitlapServer extends EmbedMockServer {

  override def port: Int                                                      = 23333
  override def run(args: List[String]): URIO[zio.ZEnv with Console, ExitCode] = super.run(args)

}
