package org.bitlap.cli.parser

import org.backuity.clist.Cli
import org.bitlap.cli.{ CLICommand, Config, bql }
import zio.UIO

import scala.collection.mutable

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
    Cli.parse(args.toArray).version("1.0-SNAPSHOT").withCommand(new bql) { bql =>
      bql.sql.tail match {
        // bsql --kvargs=key=1,key2=value2 select * from table
        case (head: String) :: (sql: List[String]) if head.startsWith("--kvargs") =>
          val propertiesStr = head.replaceFirst("--kvargs=", "")
          println(s"properties => $propertiesStr")
          val args = propertiesStr.split(",").foldLeft[mutable.Map[String, String]](new mutable.HashMap[String, String]())((map, e) => {
            if (e.contains("=")) {
              val Array(k, v) = e.split("=")
              map += k -> v
            } else {
              map += "args" -> e
            }
          }).toMap
          val conf = Config(cmd = if (args.isEmpty) "sql" else "stmt", sql = sql.mkString(" "), args)
          println(s"cli conf => $conf")
          conf
        // bsql select * from table
        case sql: List[String] if sql.isEmpty => Config(cmd = "sql", sql = sql.mkString(" "))
        case _ => Config("sql", "select 1")
      }
    }
  }
}
