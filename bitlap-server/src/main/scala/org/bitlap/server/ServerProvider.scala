/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import com.ariskk.raft.model.NodeId
import com.typesafe.config.ConfigFactory
import org.bitlap.server.http.HttpServerProvider
import org.bitlap.server.raft.{ RaftClusterServerProvider, RaftServer }
import org.bitlap.server.rpc.InternalGrpcServerProvider
import zio._

import scala.jdk.CollectionConverters.CollectionHasAsScala

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/19
 */
trait ServerProvider {

  def serverType: String

  def service(args: List[String]): URIO[zio.ZEnv, ExitCode]

}

object ServerProvider {

  private val config = ConfigFactory.load()

  private val selfConfig  = config.getConfig("raft.server.self")
  private val peersConfig = config.getConfigList("raft.server.peers")

  val selfServerConfig: RaftServer.Config = RaftServer.Config(
    NodeId.apply(selfConfig.getString("id")),
    selfConfig.getString("address"),
    selfConfig.getInt("internalPort"),
    selfConfig.getInt("port")
  )

  val peersServerConfig: IndexedSeq[RaftServer.Config] = peersConfig.asScala.toSeq.map { c =>
    RaftServer.Config(
      NodeId.apply(c.getString("id")),
      c.getString("address"),
      c.getInt("internalPort"),
      c.getInt("port")
    )
  }.toIndexedSeq

  val allServers: IndexedSeq[RaftServer.Config] = peersServerConfig ++ Seq(selfServerConfig)

  lazy val serverProviders: List[ServerProvider] = List(
    new InternalGrpcServerProvider(23333),
    new RaftClusterServerProvider(selfServerConfig, allServers),
    new HttpServerProvider(8080)
  )
}
