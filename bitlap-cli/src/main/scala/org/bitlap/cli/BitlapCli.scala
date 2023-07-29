/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.cli

import zio.*

/** 基于zio-cli实现的命令行程序，内部基于sqlline。
 *
 *  @version zio 2.0
 */
object BitlapCli extends ZIOAppDefault with BitlapInterpreter {

  override def run: ZIO[ZIOAppArgs & Scope, Nothing, Unit] =
    for {
      args <- getArgs
      _    <- bitlapApp.run(args.toList).provideLayer(ZLayer.succeed(Console.ConsoleLive)).exitCode
    } yield ()
}
