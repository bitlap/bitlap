package org.bitlap.cli

import picocli.CommandLine
import picocli.CommandLine.{Command, HelpCommand, Option}

import java.util.concurrent.Callable

/**
 * Bitlap cli command
 */
@Command(
  name = "bitlap",
  version = Array("v1.0"),
  description = Array("bitlap cli command."),
  mixinStandardHelpOptions = true,
  usageHelpAutoWidth = true,
  subcommands = Array(classOf[BitlapServerCli], classOf[BitlapSqlCli]),
)
class BitlapCli extends Callable[Int] {

  @Option(
    names = Array("-t", "--test"),
    description = Array("display version info")
  )
  var test: String = _

  override def call(): Int = {
    println(s"...... $test")
    0
  }
}

object BitlapCli {

  def main(args: Array[String]): Unit = {
    System.exit(new CommandLine(new BitlapCli()).execute(args: _*))
  }
}

