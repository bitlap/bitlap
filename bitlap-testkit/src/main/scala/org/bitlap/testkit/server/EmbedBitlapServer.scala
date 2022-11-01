/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import org.bitlap.server.ServerProvider
import zio.console.putStrLn
import zio._

/** bitlap 嵌入式服务 包含http,grpc,raft
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */

object EmbedBitlapServer extends zio.App {

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      r <- ZIO.foreach(ServerProvider.serverProviders(false))(_.service(args).fork)
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
