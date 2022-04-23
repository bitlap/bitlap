/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli

import org.bitlap.cli.extension.{ BitlapPrompt, BitlapSqlApplication }
import zio.cli.{ Args, CliApp, HelpDoc, Options, Command => ZCommand }
import org.bitlap.common.BitlapConf
import org.bitlap.common.utils.StringEx
import sqlline.{ SqlLine, SqlLineOpts }
import zio.ZIO
import zio.cli.HelpDoc.Span.text
import zio.console.putStr

import java.io.File
import scala.collection.mutable.ArrayBuffer

/**
 * This is a zio cli application, based on zio2.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/23
 */
sealed trait Command extends Product with Serializable

object Command {

  final case class Sql(
    server: String,
    user: String,
    password: String,
    args: List[String]
  ) extends Command

  final case class Server(operate: String) extends Command

  val serverHelp: HelpDoc = HelpDoc.p("Server commands, such as: start, stop, restart, status.")
  val sqlHelp: HelpDoc = HelpDoc.p("A bitlap subcommand for sql.")
  val help: HelpDoc = HelpDoc.p("bitlap cli command.")

  val serverOpt: Options[String] = Options
    .text("server")
    .withDefault("127.0.0.1:23333", "Server Addresses, separated by comma.")
    .alias("s")

  val userOpt: Options[String] = Options
    .text("user")
    .withDefault("", "User name.")
    .alias("u")

  val passwordOpt: Options[String] = Options
    .text("password")
    .withDefault("", "User password.")
    .alias("p")

  val operationOpt: Options[String] = Options.text("operate")

  val sql: ZCommand[Sql] =
    ZCommand("sql", serverOpt ++ userOpt ++ passwordOpt, Args.text("sql").repeat)
      .withHelp(sqlHelp)
      .map { input =>
        Command.Sql(input._1._1, input._1._2, input._1._3, input._2)
      }

  val server: ZCommand[Server] =
    ZCommand("server", Options.none, Args.text("operate"))
      .withHelp(serverHelp)
      .map { input =>
        Command.Server(input)
      }

  val bitlap: ZCommand[Command] = ZCommand("bitlap", Options.none, Args.none)
    .withHelp(help)
    .subcommands(sql, server)

  def sqlBuild: List[String] => String = (args: List[String]) => StringEx.trimMargin(args.mkString(" "), '"', '\'')

  val bitlapApp = CliApp.make(
    name = "Bitlap",
    version = "1.0.0",
    summary = text("bitlap cli command."),
    command = bitlap
  ) {
    case sql: Command.Sql =>
      ZIO.succeed(handleSqlCli(sql))
    case Command.Server(operate) =>
      putStr(s"Executing `bitlap server` with args: $operate")
  }

  def handleSqlCli(sql: Sql): Unit = {
    val conf = new BitlapConf()
    val projectName = conf.get(BitlapConf.PROJECT_NAME)
    val sqlArgs = ArrayBuffer(
      "-d",
      classOf[org.bitlap.Driver].getCanonicalName,
      "-u",
      s"jdbc:bitlap://${sql.server}/default",
      "-n",
      sql.user,
      "-p",
      sql.password,
      "-ac",
      classOf[BitlapSqlApplication].getCanonicalName
    )
    val defaultSql = sqlBuild(sql.args)
    if (!StringEx.nullOrBlank(defaultSql)) {
      sqlArgs ++= Array("-e", defaultSql)
    }
    // sql line REPL or execute sql directly
    System.setProperty("x.sqlline.basedir", getHistoryPath(projectName))
    val line = new SqlLine()
    line.getOpts.set(BitlapPrompt, projectName)

    val status = line.begin(sqlArgs.toArray, null, false)
    if (!java.lang.Boolean.getBoolean(SqlLineOpts.PROPERTY_NAME_EXIT)) {
      System.exit(status.ordinal)
    }
  }

  private def getHistoryPath(projectName: String): String = {
    val home = System.getProperty("user.home")
    val os = System.getProperty("os.name").toLowerCase
    val child = if (os.contains("windows")) {
      projectName
    } else {
      s".$projectName" // default is: ~/.bitlap
    }
    new File(home, child).getAbsolutePath
  }

}
