/* Copyright (c) 2023 bitlap.org */
package org.bitlap.cli

import java.io.*

import scala.collection.mutable.ArrayBuffer

import org.bitlap.cli.BitlapInterpreter.CliCommands.*
import org.bitlap.cli.Command.{ Server, Sql }
import org.bitlap.cli.interactive.BitlapSqlApplication
import org.bitlap.cli.interactive.BitlapSqlLineProperty.BitlapPrompt
import org.bitlap.common.BitlapConf
import org.bitlap.common.utils.StringEx

import sqlline.*
import zio.{ Console, System as ZSystem, * }
import zio.cli.{ Command as ZioCliCommand, * }
import zio.cli.HelpDoc.Span.text

/** 基于zio-cli实现的命令行解释器
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/24
 */
trait BitlapInterpreter {

  def sqlBuild: List[String] => String = (args: List[String]) =>
    StringEx.trimMargin(args.map(_.trim).mkString(" "), '"', '\'')

  import BitlapInterpreter.bitlap

  val bitlapApp: CliApp[Console, IOException, Command] = CliApp.make(
    name = "Bitlap",
    version = "0.4.0-SNAPSHOT",
    summary = text("bitlap cli command."),
    command = bitlap
  ) {
    case sql: Command.Sql =>
      Console.printLine(s"Executing 'bitlap sql' with args: ${sqlBuild(sql.args)}") *> ZIO.succeed(handleSqlCli(sql))
    case Command.Server(operate) =>
      Console.printLine(s"Executing 'bitlap server' with operate: $operate")
  }

  private def handleSqlCli(sql: Sql): Int = {
    println(s"Connecting to bitlap with args: $sql")
    val conf        = new BitlapConf()
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
    if !StringEx.nullOrBlank(defaultSql) then {
      sqlArgs ++= Array("-e", defaultSql)
    }
    // sql line REPL or execute sql directly
    System.setProperty("x.sqlline.basedir", getHistoryPath(projectName))
    val line = new SqlLine()
    line.getOpts.set(BitlapPrompt, projectName)
    val status = line.begin(sqlArgs.toArray, null, false)
    if !java.lang.Boolean.getBoolean(SqlLineOpts.PROPERTY_NAME_EXIT) then {
      // System.exit(status.ordinal)
      status.ordinal()
    } else {
      0
    }
  }

  private def getHistoryPath(projectName: String): String = {
    val home = System.getProperty("user.home")
    val os   = System.getProperty("os.name").toLowerCase
    val child = if os.contains("windows") then {
      projectName
    } else {
      s".$projectName" // default is: ~/.bitlap
    }
    new File(home, child).getAbsolutePath
  }

}

object BitlapInterpreter {

  import org.bitlap.cli.BitlapInterpreter.CliOptions.*

  val bitlap: ZioCliCommand[Command] = ZioCliCommand("bitlap", Options.none, Args.none)
    .withHelp(help)
    .subcommands(sql, server)

  object CliCommands {

    val sql: ZioCliCommand[Sql] =
      ZioCliCommand("sql", hostOpt ++ userOpt ++ passwordOpt, Args.text.*)
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

    val serverHelp: HelpDoc = HelpDoc.p("bitlap server commands, such as: start, stop, restart.")
    val sqlHelp: HelpDoc    = HelpDoc.p("bitlap sql command.")
    val help: HelpDoc       = HelpDoc.p("bitlap cli command.")

    val hostOpt = Options
      .text("server")
      .withDefault("127.0.0.1:23333")
      .alias("s")

    val userOpt: Options[String] = Options
      .text("user")
      .withDefault("")
      .alias("u")

    val passwordOpt: Options[String] = Options
      .text("password")
      .withDefault("")
      .alias("p")
  }

}
