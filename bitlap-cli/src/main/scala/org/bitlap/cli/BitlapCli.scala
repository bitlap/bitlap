/* Copyright (c) 2023 bitlap.org */
package org.bitlap.cli

import zio._

/** 基于zio-cli实现的命令行程序，内部基于sqlline。
 *
 *  @version zio 1.0
 */
object BitlapCli extends zio.ZIOAppDefault with BitlapInterpreter {

  override def run =
    bitlapApp.run(Nil).provideLayer(ZLayer.succeed(Console.ConsoleLive)).exitCode

}
