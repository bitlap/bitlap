package org.bitlap.cli

import picocli.CommandLine
import picocli.CommandLine.{ Command, HelpCommand, Option }

import java.util.concurrent.Callable

/**
 *
 * @author 梦境迷离
 * @since 2021/12/18
 * @version 1.0
 */
@Command(
  name = "bitlap",
  version = Array("bitlap 1.0"),
  description = Array("Connect to bitlap cluster."),
  usageHelpAutoWidth = true,
  subcommands = Array(classOf[BitlapServer], classOf[BitlapSql], classOf[HelpCommand]),
)
class Bitlap extends Callable[Int] {

  @Option(names = Array("-v", "--version"),
    versionHelp = true,
    description = Array("display version info")
  )
  var version = false

  override def call(): Int = {
    println(" >")
    0
  }

}

object Bitlap {

  def main(args: Array[String]): Unit = {
    val safeVarargs = Array("sql", "help")
    System.exit(new CommandLine(new Bitlap()).execute(safeVarargs: _*))
  }
}

