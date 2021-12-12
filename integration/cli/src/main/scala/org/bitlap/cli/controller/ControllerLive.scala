package org.bitlap.cli.controller

import org.bitlap.cli.parser.CLICommandParser
import org.bitlap.cli.{ CLICommand, CLIContext }
import zio.UIO

/**
 * 控制层
 * 输入和上下文转化
 *
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 */
trait ControllerLive extends Controller {

  val cliCommandParser: CLICommandParser.Service[Any]

  val controller: Controller.Service[Any] = new Controller.Service[Any] {

    override def process(input: String, cliContext: CLIContext): UIO[CLIContext] = {
      cliCommandParser.parse(input).map {
        case CLICommand.ExecuteStatement(sql, args) =>
          println(s"stmt cmd => sql")
          println(s"stmt args => $args")
          cliContext
        case CLICommand.ExecuteNativeSQL(sql, args) =>
          println(s"sql cmd => sql")
          println(s"sql args => $args")
          cliContext
        case CLICommand.ExecuteDDL(sql, args) => cliContext
        case CLICommand.Invalid => cliContext
      }
    }
  }
}
