/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import org.bitlap.network.ServerType
import org.bitlap.server.http.HttpServerProvider
import org.bitlap.server.raft.{ RaftServerConfig, RaftServerProvider }
import org.bitlap.server.rpc.InternalGrpcServerProvider
import zio._

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/19
 */
trait ServerProvider {

  /** bitlap 服务类型
   *
   *  @return
   */
  def serverType: ServerType

  /** @param args
   *    现在我们并未使用命令行参数，但将来可能会使用
   *  @return
   */
  def service(args: List[String]): URIO[zio.ZEnv, ExitCode]

}

object ServerProvider {

  def serverProviders(http: Boolean): List[ServerProvider] =
    (if (http) List(new HttpServerProvider(8081)) else List()) ++ List(
      new InternalGrpcServerProvider(23333),
      new RaftServerProvider(RaftServerConfig.raftServerConfig)
    )
}
