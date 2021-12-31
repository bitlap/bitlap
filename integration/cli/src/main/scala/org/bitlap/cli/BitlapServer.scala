package org.bitlap.cli

import picocli.CommandLine.{ Command, HelpCommand, Option, Parameters }

/**
 *
 * @author 梦境迷离
 * @since 2021/12/31
 * @version 1.0
 */
@Command(
  name = "server",
  description = Array("A bitlap subcommand for server."),
  subcommands = Array(classOf[HelpCommand])
)
class BitlapServer extends Runnable {

  @Option(names = Array("-c", "--conf"),
    paramLabel = "CONF",
    description = Array("Config file path"),
    required = false,
    hideParamSyntax = true,
    defaultValue = "conf/bitlap.setting"
  )
  var config: String = "conf/bitlap.setting"

  @Parameters(paramLabel = "OPERATE", description = Array("start or stop"))
  var args: String = _

  override def run(): Unit = {
    println(s"args:$args, conf:$config")
  }

}