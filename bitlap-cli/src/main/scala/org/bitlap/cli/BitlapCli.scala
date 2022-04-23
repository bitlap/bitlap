/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli

import zio.{ ExitCode, URIO }

/**
 * Bitlap cli command
 */
object BitlapCli extends zio.App {

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    Command.bitlapApp.run(args)

}
