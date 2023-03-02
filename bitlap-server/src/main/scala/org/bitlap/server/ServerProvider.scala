/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server

import org.bitlap.network.ServerType
import org.bitlap.server.http.HttpServerProvider
import org.bitlap.server.raft._
import org.bitlap.server.rpc.GrpcServerProvider
import zio._
import org.bitlap.common.BitlapConf
import org.bitlap.client._

/** bitlap 抽象服务接口
 *  @author
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

  private val httpPort   = BitlapServerContext.globalConf.get(BitlapConf.HTTP_SERVER_ADDRESS).extractServerAddress.port
  private val serverPort = BitlapServerContext.globalConf.get(BitlapConf.NODE_BIND_HOST).extractServerAddress.port

  /** 所有bitlap 内部服务
   *  @param http
   *    是否启动 HTTP 服务
   *  @return
   */
  def serverProviders(http: Boolean): List[ServerProvider] =
    (if (http) List(new HttpServerProvider(httpPort)) else List()) ++ List(
      new GrpcServerProvider(serverPort),
      new RaftServerProvider(RaftServerConfig.raftServerConfig)
    )
}
