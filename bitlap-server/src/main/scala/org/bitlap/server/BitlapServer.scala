/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import org.bitlap.server.rpc.Services
import scalapb.zio_grpc.{ ServerMain, ServiceList }
import zio.console.Console
import zio.{ ExitCode, URIO }
import zio.ZIO
import zio.console.putStrLn

/** @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
class Server(val serverPort: Int) extends ServerMain {

  override def port: Int = serverPort

  def services: ServiceList[zio.ZEnv] = ServiceList.addM(Services.zioLive) // 可以随意更换实现

}

object BitlapServer extends Server(23333) {

  override def run(args: List[String]): URIO[zio.ZEnv with Console, ExitCode] =
    (for {
      _ <- putStrLn("""
                      |    __    _ __  __          
                      |   / /_  (_) /_/ /___ _____ 
                      |  / __ \/ / __/ / __ `/ __ \
                      | / /_/ / / /_/ / /_/ / /_/ /
                      |/_.___/_/\__/_/\__,_/ .___/ 
                      |                   /_/   
                      |""".stripMargin)
      r <- super.run(args)
    } yield r).foldM(
      e => ZIO.fail(e).exitCode,
      _ => ZIO.effectTotal(ExitCode.success)
    )

}
