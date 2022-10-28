/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft
import com.alipay.sofa.jraft.{ Lifecycle, RaftGroupService }
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.NodeOptions
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory
import com.alipay.sofa.jraft.util.internal.ThrowUtil
import org.apache.commons.io.FileUtils

import java.io.IOException
import java.nio.file.Paths
import java.util.concurrent.CopyOnWriteArrayList
import com.alipay.sofa.jraft.Node
import com.alipay.sofa.jraft.conf.Configuration
import java.io.File
import org.bitlap.server.raft.ElectionNode
import org.slf4j.LoggerFactory

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/28
 */
final class ElectionNode extends Lifecycle[ElectionNodeOptions] {

  private lazy val LOG = LoggerFactory.getLogger(classOf[ElectionNode])

  private val listeners                          = new CopyOnWriteArrayList[LeaderStateListener]
  private var raftGroupService: RaftGroupService = _
  private var node: Node                         = _
  private var fsm: ElectionOnlyStateMachine      = _

  private var started = false

  override def init(opts: ElectionNodeOptions): Boolean = {
    if (this.started) {
      LOG.info("[ElectionNode: {}] already started.", opts.serverAddress)
      return true
    }
    // node options
    val nodeOpts = if (opts.nodeOptions == null) new NodeOptions else opts.nodeOptions
    this.fsm = new ElectionOnlyStateMachine(this.listeners)
    nodeOpts.setFsm(this.fsm)
    val initialConf = new Configuration
    if (!initialConf.parse(opts.initialServerAddressList))
      throw new IllegalArgumentException("Fail to parse initConf: " + opts.initialServerAddressList)

    // Set the initial cluster configuration
    nodeOpts.setInitialConf(initialConf)
    val dataPath = opts.dataPath
    try FileUtils.forceMkdir(new File(dataPath))
    catch {
      case e: IOException =>
        e.printStackTrace()
        LOG.error("Fail to make dir for dataPath {}.", dataPath)
        return false
    }
    nodeOpts.setLogUri(Paths.get(dataPath, "log").toString)
    // Metadata, required
    nodeOpts.setRaftMetaUri(Paths.get(dataPath, "meta").toString)
    val groupId  = opts.groupId
    val serverId = new PeerId
    if (!serverId.parse(opts.serverAddress))
      throw new IllegalArgumentException("Fail to parse serverId: " + opts.serverAddress)
    val rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint)
    this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOpts, rpcServer)
    this.node = this.raftGroupService.start
    if (this.node != null) this.started = true
    this.started
  }

  override def shutdown(): Unit = {
    if (!this.started) return
    if (this.raftGroupService != null) {
      this.raftGroupService.shutdown()
      try this.raftGroupService.join()
      catch {
        case e: InterruptedException =>
          ThrowUtil.throwException(e)
      }
    }
    this.started = false
    LOG.info("[ElectionNode] shutdown successfully: {}.", this)
  }

  def getNode: Node = node

  def getFsm: ElectionOnlyStateMachine = fsm

  def isStarted: Boolean = started

  def isLeader: Boolean = this.fsm.isLeader

  def addLeaderStateListener(listener: LeaderStateListener): Unit =
    this.listeners.add(listener)

}
