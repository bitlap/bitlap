package org.bitlap.cli

import picocli.CommandLine.{ Command, HelpCommand, Option, Parameters }

/**
 *
 * @author 梦境迷离
 * @since 2021/12/31
 * @version 1.0
 */
@Command(
  name = "sql",
  description = Array("A bitlap subcommand for sql."),
  subcommands = Array(classOf[HelpCommand])
)
class BitlapSql extends Runnable {

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

  @Parameters(paramLabel = "SQL", description = Array("SQL to execute"), defaultValue = "")
  var args: String = _

  override def run(): Unit = {
    println(s"host:$host, port:$port, user:$user, password:$password")
  }

}