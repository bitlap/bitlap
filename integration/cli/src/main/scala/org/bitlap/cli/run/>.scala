package org.bitlap.cli.run

import org.bitlap.cli.CLIContext
import zio.{ RIO, ZIO }

/**
 * 用于初始化调用
 *
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 */
object > extends RunSQL.Service[RunSQL] {
  override def exec(cliContext: CLIContext): RIO[RunSQL, CLIContext] = {
    ZIO.accessM(_.runSQL.exec(cliContext))
  }
}
