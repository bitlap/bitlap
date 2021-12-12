package org.bitlap.cli.run

import org.bitlap.cli.CLIContext
import org.bitlap.cli.controller.Controller
import org.bitlap.cli.terminal.Terminal
import zio.RIO

/**
 * 逻辑
 *
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 */
trait RunSQLLive extends RunSQL {

  val controller: Controller.Service[Any]
  val terminal: Terminal.Service[Any]

  override final val runSQL = new RunSQL.Service[Any] {

    override def exec(cliContext: CLIContext): RIO[Any, CLIContext] = {
      for {
        // 解析输入
        input <- terminal.getUserInput
        // 处理结果
        result <- controller.process(input, cliContext)
        // 展示结果
        _ <- terminal.display(result)
      } yield result
    }
  }
}
