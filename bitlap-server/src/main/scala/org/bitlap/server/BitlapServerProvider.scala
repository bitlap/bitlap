/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import zio.{ ExitCode, URIO }
import org.bitlap.server.http.HttpServerProvider
import org.bitlap.server.raft.RaftClusterServerProvider
import org.bitlap.server.rpc.InternalGrpcServerProvider

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/19
 */
trait BitlapServerProvider {

  def serverType: String

  def service(args: List[String]): URIO[zio.ZEnv, ExitCode]

}

object BitlapServerProvider {

  lazy val serverProviders: List[BitlapServerProvider] = List(
    new InternalGrpcServerProvider(23333),
    RaftClusterServerProvider,
    new HttpServerProvider(8080)
  )
}
