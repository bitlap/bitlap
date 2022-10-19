/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server
import zio.console.putStrLn
import zio._

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/19
 */
object BitlapServer extends zio.App {

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      _ <- ZIO.collectAll(BitlapServerProvider.serverProviders.map(_.service(args)))
      _ <- putStrLn("""
                      |    __    _ __  __          
                      |   / /_  (_) /_/ /___ _____ 
                      |  / __ \/ / __/ / __ `/ __ \
                      | / /_/ / / /_/ / /_/ / /_/ /
                      |/_.___/_/\__/_/\__,_/ .___/ 
                      |                   /_/   
                      |""".stripMargin)
    } yield ()).foldM(
      e => ZIO.fail(e).exitCode,
      _ => ZIO.effectTotal(ExitCode.success)
    )
}
