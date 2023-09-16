/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.cli

import zio.*

/** A command line program based on zio-cli implementation, internally based on sqlline.
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
