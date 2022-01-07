package org.bitlap.cli

import org.bitlap.cli.extension.BitlapSqlApplication
import org.bitlap.common.BitlapConf
import org.bitlap.common.utils.StringEx
import org.bitlap.tools.apply
import picocli.CommandLine.{ Command, Option, Parameters }
import sqlline.{ SqlLine, SqlLineOpts }

import java.io.File
import java.lang.{ Boolean => JBoolean }

/**
 * sql command line
 */
@Command(
  name = "sql",
  description = Array("A bitlap subcommand for sql."),
  mixinStandardHelpOptions = true,
  usageHelpAutoWidth = true,
)
@apply
class BitlapSqlCli extends Runnable {

  @Option(
    names = Array("-s", "--server"),
    paramLabel = "SERVER",
    description = Array("Server Address."),
    defaultValue = "localhost:23333",
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
    interactive = true,
  )
  var password: String = _

  @Parameters(
    paramLabel = "SQL",
    description = Array("SQL to execute."),
    defaultValue = "",
  )
  var args: Array[String] = _
  lazy val sql: String = StringEx.trimMargin(this.args.mkString(" "), '"', '\'')

  override def run(): Unit = ()
}

object BitlapSqlCli {

  def main(args: Array[String]): Unit = {
    BitlapSqlExecutor.parseArgs(args: _*)
    val cmd = BitlapSqlExecutor.getCommand[BitlapSqlCli]
    cmd.sql match {
      // sql line REPL
      case "" =>
        val conf = new BitlapConf()
        val projectName = conf.get(BitlapConf.PROJECT_NAME)
        System.setProperty("x.sqlline.basedir", getHistoryPath(projectName))
        BitlapSqlApplication.conf.set(conf)
        val line = new SqlLine()
        val status = line.begin(
          Array(
            "-d", "org.h2.Driver",
            "-u", "jdbc:h2:mem:",
            "-n", "sa",
            "-p", "",
            "-ac", classOf[BitlapSqlApplication].getCanonicalName)
          , null, false
        )
        if (!JBoolean.getBoolean(SqlLineOpts.PROPERTY_NAME_EXIT)) {
          System.exit(status.ordinal)
        }

      // execute sql directly
      case sql =>

    }
  }

  private def getHistoryPath(projectName: String): String = {
    val home = System.getProperty("user.home")
    val os = System.getProperty("os.name").toLowerCase
    val child = if (os.contains("windows")) {
      projectName
    } else {
      s".$projectName"
    }
    new File(home, child).getAbsolutePath
  }
}
