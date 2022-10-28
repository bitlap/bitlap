/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import zio.console.putStrLn
import zio._

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/19
 */
object BitlapServer extends zio.App {

  // 在java 9以上运行时，需要JVM参数：--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      r <- ZIO.collectAll(ServerProvider.serverProviders.map(_.service(args).fork))
      _ <- putStrLn("""
                      |    __    _ __  __
                      |   / /_  (_) /_/ /___ _____
                      |  / __ \/ / __/ / __ `/ __ \
                      | / /_/ / / /_/ / /_/ / /_/ /
                      |/_.___/_/\__/_/\__,_/ .___/
                      |                   /_/
                      |""".stripMargin)
      _ <- ZIO.foreach_(r)(_.join)
    } yield ()).foldM(
      e => ZIO.fail(e).exitCode,
      _ => ZIO.effectTotal(ExitCode.success)
    )
}
