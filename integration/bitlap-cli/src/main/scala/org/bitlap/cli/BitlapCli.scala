/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli

import org.bitlap.tools.apply
import picocli.CommandLine.Command

/**
 * Bitlap cli command
 */
@Command(
  name = "bitlap",
  version = Array("v1.0"),
  description = Array("bitlap cli command."),
  mixinStandardHelpOptions = true,
  usageHelpAutoWidth = true,
  subcommands = Array(classOf[BitlapServerCli], classOf[BitlapSqlCli])
)
@apply
class BitlapCli

object BitlapCli {
  def main(args: Array[String]): Unit =
    System.exit(BitlapExecutor.<<?(args))
}
