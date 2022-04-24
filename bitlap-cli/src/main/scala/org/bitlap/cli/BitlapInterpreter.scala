/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli

import org.bitlap.cli.BitlapInterpreter.CliCommands.{ server, sql }
import org.bitlap.cli.Command.{ Server, Sql }
import org.bitlap.cli.interactive.BitlapSqlApplication
import org.bitlap.cli.interactive.BitlapSqlLineProperty.BitlapPrompt
import org.bitlap.common.BitlapConf
import org.bitlap.common.utils.StringEx
import sqlline.{ SqlLine, SqlLineOpts }
import zio.ZIO
import zio.cli.HelpDoc.Span.text
import zio.cli.{ Args, CliApp, HelpDoc, Options, Command => ZioCliCommand }
import zio.console.{ putStrLn, Console }

import java.io.{ File, IOException }
import scala.collection.mutable.ArrayBuffer

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/24
 */
trait BitlapInterpreter {

  def sqlBuild: List[String] => String = (args: List[String]) => args.map(_.trim).mkString("'", " ", "'")

  import BitlapInterpreter.bitlap

  val bitlapApp: CliApp[Console, IOException, Command] = CliApp.make(
    name = "Bitlap",
    version = "1.0.0",
    summary = text("bitlap cli command."),
    command = bitlap
  ) {
    case sql: Command.Sql =>
      putStrLn(s"Executing `bitlap sql` with args: ${sqlBuild(sql.args)}") andThen ZIO.succeed(handleSqlCli(sql))
    case Command.Server(operate) =>
      putStrLn(s"Executing `bitlap server` with args: $operate")
  }

  private def handleSqlCli(sql: Sql): Unit = {
    val conf = new BitlapConf()
    println(s"Conf: ${conf.getConf}, sql: ${sqlBuild(sql.args)}")
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
    println(s"SqlLine args: $sqlArgs")
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
object BitlapInterpreter {

  import org.bitlap.cli.BitlapInterpreter.CliOptions._

  val bitlap: ZioCliCommand[Command] = ZioCliCommand("bitlap", Options.none, Args.none)
    .withHelp(help)
    .subcommands(sql, server)

  object CliCommands {

    // sql -h localhost -u 123 -p 123 show tables
    val sql: ZioCliCommand[Sql] =
      ZioCliCommand("sql", serverOpt ++ userOpt ++ passwordOpt, Args.text.*)
        .withHelp(sqlHelp)
        .map { input =>
          Command.Sql(input._1._1, input._1._2, input._1._3, input._2)
        }

    val server: ZioCliCommand[Server] =
      ZioCliCommand("server", Options.none, Args.text("operate"))
        .withHelp(serverHelp)
        .map { input =>
          Command.Server(input)
        }
  }

  object CliOptions {

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
  }

}
