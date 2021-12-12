package org.bitlap.cli.parser

import org.bitlap.cli.CLICommand
import zio.ZIO

/**
 * 输入解析器
 *
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 */
trait CLICommandParser {
  // 只是service的容器
  val cliCommandParser: CLICommandParser.Service[Any]
}

object CLICommandParser {

  trait Service[R] {
    def parse(input: String): ZIO[R, Nothing, CLICommand]
  }
}