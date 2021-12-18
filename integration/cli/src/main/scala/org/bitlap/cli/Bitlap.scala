package org.bitlap.cli

import picocli.CommandLine
import picocli.CommandLine.{ Command, Option }

import java.util.concurrent.Callable

/**
 *
 * @author 梦境迷离
 * @since 2021/12/18
 * @version 1.0
 */
@Command(
  name = "bitlap",
  mixinStandardHelpOptions = true,
  version = Array("bitlap 1.0"),
  description = Array("Connect to bitlap cluster."),
  usageHelpAutoWidth = true
)
class Bitlap extends Callable[Int] {

  @Option(names = Array("-h", "--host"),
    paramLabel = "HOST",
    description = Array("Server host"),
    required = true,
    hideParamSyntax = true,
    defaultValue = "localhost"
  )
  var host: String = "localhost"

  @Option(names = Array("-P", "--port"),
    paramLabel = "PORT",
    description = Array("Server port"),
    required = true,
    hideParamSyntax = true,
    defaultValue = "23333"
  )
  var port: Int = 23333

  @Option(names = Array("-u", "--user"),
    paramLabel = "USERNAME",
    description = Array("User name"),
    required = false,
    hideParamSyntax = true
  )
  var user: String = _

  @Option(names = Array("-p", "--password"),
    paramLabel = "PASSWORD",
    description = Array("User password"),
    required = false,
    hideParamSyntax = true
  )
  var password: String = _

  @Option(names = Array("--help"),
    usageHelp = true,
    description = Array("display a help message")
  )
  var help: Boolean = false

  @Option(names = Array("-v", "--version"),
    versionHelp = true,
    description = Array("display version info")
  )
  var version = false

  override def call(): Int = {
    0
  }

}

object Bitlap {

  def main(args: Array[String]): Unit = {
    System.exit(new CommandLine(new Bitlap()).execute(args: _*))
  }
}

