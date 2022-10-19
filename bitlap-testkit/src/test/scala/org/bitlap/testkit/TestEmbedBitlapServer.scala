/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit

import org.bitlap.testkit.server.EmbedMockServer

import zio._
import zio.console.Console

object TestEmbedBitlapServer extends EmbedMockServer {

  override def port: Int                                                      = 23333
  override def run(args: List[String]): URIO[zio.ZEnv with Console, ExitCode] = super.run(args)

}
