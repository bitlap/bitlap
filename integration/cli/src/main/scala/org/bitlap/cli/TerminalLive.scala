package org.bitlap.cli

import zio.console.Console

/**
 * cli
 *
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 */
trait TerminalLive extends Terminal {

  val console: Console.Service

  final val terminal = new Terminal.Service[Any] {

    lazy val getUserInput = console.getStrLn.orDie

    def display(cliContext: CLIContext) =
      for {
        _ <- console.putStr(TerminalLive.ANSI_CLEARSCREEN)
        _ <- console.putStrLn(cliContext.toString)
      } yield ()
  }
}

object TerminalLive {
  val ANSI_CLEARSCREEN: String = "\u001b[H\u001b[2J"
}
