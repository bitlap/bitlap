/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import com.typesafe.config.ConfigFactory
import org.bitlap.server.http.HttpServerProvider
import org.bitlap.server.rpc.InternalGrpcServerProvider
import zio._
import org.bitlap.server.raft.{ RaftServerConfig, RaftServerProvider }
import org.bitlap.network.ServerType

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

  private lazy val config = ConfigFactory.load().getConfig("bitlap.server.raft")
  private val raftServerConfig = RaftServerConfig(
    config.getString("dataPath"),
    config.getString("groupId"),
    config.getString("serverAddress"),
    config.getString("initialServerAddressList")
  )

  lazy val serverProviders: List[ServerProvider] = List(
    new InternalGrpcServerProvider(23332),
    new HttpServerProvider(8081),
    new RaftServerProvider(raftServerConfig)
  )
}
