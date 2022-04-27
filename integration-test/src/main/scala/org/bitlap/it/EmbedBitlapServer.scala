/* Copyright (c) 2022 bitlap.org */
package org.bitlap.it

import org.bitlap.testkit.server.MockServer
import zio.console.Console
import zio.{ ExitCode, URIO }

object EmbedBitlapServer extends MockServer {

  override def port: Int = 23333
  override def run(args: List[String]): URIO[zio.ZEnv with Console, ExitCode] = super.run(args)

}
