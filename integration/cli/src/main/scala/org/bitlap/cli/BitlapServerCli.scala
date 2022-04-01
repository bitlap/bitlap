/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli

import org.bitlap.tools.apply
import picocli.CommandLine.{ Command, HelpCommand, Option, Parameters }

/**
 * @author 梦境迷离
 * @since 2021/12/31
 * @version 1.0
 */
@Command(
  name = "server",
  description = Array("A bitlap subcommand for server."),
  mixinStandardHelpOptions = true,
  usageHelpAutoWidth = true
)
@apply
class BitlapServerCli extends Runnable {

  @Parameters(paramLabel = "OPERATE", description = Array("start, stop, status."))
  var args: String = _

  override def run(): Unit = ()
}

object BitlapServerCli {
  def main(args: Array[String]): Unit =
    System.exit(BitlapServerExecutor.<<?(args))
}
