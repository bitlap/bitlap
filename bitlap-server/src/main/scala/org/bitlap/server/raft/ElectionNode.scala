/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.server.raft

import java.io.*
import java.nio.file.Paths
import java.util.concurrent.CopyOnWriteArrayList

import org.bitlap.server.BitlapContext
import org.bitlap.server.raft.rpc.GetServerMetadataProcessor

import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import com.alipay.sofa.jraft.*
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory
import com.alipay.sofa.jraft.util.internal.ThrowUtil

/** raft 选主
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/10/28
 */
final class ElectionNode extends Lifecycle[ElectionNodeOptions]:

  private lazy val LOG = LoggerFactory.getLogger(classOf[ElectionNode])

  private val listeners                          = new CopyOnWriteArrayList[LeaderStateListener]
  private var raftGroupService: RaftGroupService = _

  var node: Node                    = _
  var fsm: ElectionOnlyStateMachine = _

  private var started = false

  override def init(opts: ElectionNodeOptions): Boolean = {
    if this.started then {
      LOG.info("[ElectionNode: {}] already started.", opts.serverAddress)
      return true
    }
    // node options
    val nodeOpts = opts.nodeOptions
    this.fsm = new ElectionOnlyStateMachine(this.listeners)
    nodeOpts.setFsm(this.fsm)
    val initialConf = new Configuration
    if !initialConf.parse(opts.initialServerAddressList) then
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
    if !serverId.parse(opts.serverAddress) then
      throw new IllegalArgumentException("Fail to parse serverId: " + opts.serverAddress)
    val rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint)
    rpcServer.registerProcessor(new GetServerMetadataProcessor(null, BitlapContext.globalConf))
    this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOpts, rpcServer)
    this.node = this.raftGroupService.start
    if this.node != null then this.started = true
    this.started
  }

  override def shutdown(): Unit = {
    if !this.started then return
    if this.raftGroupService != null then {
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

  def addLeaderStateListener(listener: => LeaderStateListener): Unit =
    this.listeners.add(listener)
