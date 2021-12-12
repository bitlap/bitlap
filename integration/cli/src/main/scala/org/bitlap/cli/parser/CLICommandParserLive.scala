package org.bitlap.cli.parser

import org.backuity.clist.Cli
import org.bitlap.cli.{ CLICommand, Config, SQL }
import zio.UIO

/**
 * 命令行解析层
 *
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 */
trait CLICommandParserLive extends CLICommandParser {

  val cliCommandParser: CLICommandParser.Service[Any] = new CLICommandParser.Service[Any] {
    def parse(input: String): UIO[CLICommand] = {
      UIO.succeed(parseArgs(input.split(" "))) map {
        case Some(conf) if conf.cmd == "stmt" => CLICommand.ExecuteStatement(conf.sql, conf.kwargs)
        case Some(conf) if conf.cmd == "ddl" => CLICommand.ExecuteDDL(conf.sql, conf.kwargs)
        case Some(conf) if conf.cmd == "sql" => CLICommand.ExecuteNativeSQL(conf.sql, conf.kwargs)
        case _ => CLICommand.Invalid
      }
    }
  }


  private def parseArgs(args: Seq[String]): Option[Config] = {
    Cli.parse(args.toArray).withCommand(new SQL) { sql =>
      Config(cmd = sql.s.head, sql = sql.s.tail.tail.tail.mkString(" "), Map("args" -> sql.kvargs))
    }
  }
}
