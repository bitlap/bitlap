/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli

import org.bitlap.cli.extension.BitlapSqlApplication
import org.bitlap.common.BitlapConf
import org.bitlap.common.utils.StringEx
import org.bitlap.tools.apply
import picocli.CommandLine.{ Command, Option, Parameters }
import sqlline.{ SqlLine, SqlLineOpts }

import java.io.File
import java.lang.{ Boolean => JBoolean }
import scala.collection.mutable.ArrayBuffer

/**
 * sql command line
 */
@Command(
  name = "sql",
  description = Array("A bitlap subcommand for sql."),
  mixinStandardHelpOptions = true,
  usageHelpAutoWidth = true
)
@apply
class BitlapSqlCli extends Runnable {

  @Option(
    names = Array("-s", "--server"),
    paramLabel = "SERVER",
    description = Array("Server Addresses, separated by comma."),
    defaultValue = "127.0.0.1:23333"
  )
  var server: String = _

  @Option(
    names = Array("-u", "--user"),
    paramLabel = "USERNAME",
    description = Array("User name."),
    defaultValue = ""
  )
  var user: String = _

  @Option(
    names = Array("-p", "--password"),
    paramLabel = "PASSWORD",
    description = Array("User password."),
    defaultValue = "",
    interactive = true
  )
  var password: String = _

  @Parameters(
    paramLabel = "SQL",
    description = Array("SQL to execute."),
    defaultValue = ""
  )
  var args: Array[String] = _
  def sql: String = StringEx.trimMargin(this.args.mkString(" "), '"', '\'')

  override def run(): Unit = ()
}

object BitlapSqlCli {

  def main(args: Array[String]): Unit = {
    BitlapSqlExecutor.parseArgs(args: _*)
    if (BitlapSqlExecutor.isUsageHelpRequested || BitlapSqlExecutor.isVersionHelpRequested) {
      System.exit(BitlapSqlExecutor.<<?(args))
    }

    val conf = new BitlapConf()
    val projectName = conf.get(BitlapConf.PROJECT_NAME)
    val cmd = BitlapSqlExecutor.getCommand[BitlapSqlCli]
    val sqlArgs = ArrayBuffer(
      "-d",
      classOf[org.bitlap.Driver].getCanonicalName,
      "-u",
      s"jdbc:bitlap://${cmd.server}/default",
      "-n",
      cmd.user,
      "-p",
      cmd.password,
      "-ac",
      classOf[BitlapSqlApplication].getCanonicalName
    )
    val defaultSql = cmd.sql
    if (!StringEx.nullOrBlank(defaultSql)) {
      sqlArgs ++= Array("-e", defaultSql)
    }
    // sql line REPL or execute sql directly
    System.setProperty("x.sqlline.basedir", getHistoryPath(projectName))
    BitlapSqlApplication.conf.set(conf)
    val line = new SqlLine()
    val status = line.begin(sqlArgs.toArray, null, false)
    if (!JBoolean.getBoolean(SqlLineOpts.PROPERTY_NAME_EXIT)) {
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
