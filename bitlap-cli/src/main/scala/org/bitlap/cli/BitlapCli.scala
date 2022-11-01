/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli

import zio._

/** 基于zio-cli实现的命令行程序，内部基于sqlline。
 *  @version zio 1.0
 */
object BitlapCli extends zio.App with BitlapInterpreter {

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    bitlapApp.run(args).exitCode

}
