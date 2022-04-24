/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli

import zio.{ ExitCode, URIO }

/**
 * Bitlap cli command
 */
object BitlapCli extends zio.App with BitlapInterpreter {

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    bitlapApp.run(args)

}
