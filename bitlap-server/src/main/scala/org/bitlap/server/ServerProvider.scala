/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import org.bitlap.server.http.HttpServerProvider
import org.bitlap.server.rpc.InternalGrpcServerProvider
import zio._

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/19
 */
trait ServerProvider {

  def serverType: String

  def service(args: List[String]): URIO[zio.ZEnv, ExitCode]

}

object ServerProvider {

  lazy val serverProviders: List[ServerProvider] = List(
    new InternalGrpcServerProvider(23333),
    new HttpServerProvider(8080)
  )
}
