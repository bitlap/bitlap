/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import com.alipay.sofa.jraft.Node
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.NodeOptions
import org.apache.commons.io.FileUtils
import org.bitlap.common.utils.StringEx
import org.bitlap.common.{ BitlapConf, LifeCycleThread }
import org.bitlap.network.RPC
import org.bitlap.server.raft.MetaStateMachine
import org.bitlap.server.rpc.processor.JdbcProcessors
import org.bitlap.tools.apply

import java.io.File

/**
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
@apply
class BitlapServerEndpoint(private val conf: BitlapConf) extends LifeCycleThread("BitlapServerEndpoint", true) {

  private var node: Node = _

  override def run(): Unit = {
    val processors = JdbcProcessors().processors
    this.node = RPC.newServer(conf, processors).start(extractOptions())
    log.info(s"Started server at: ${node.getNodeId.getPeerId}")
  }

  private def extractOptions(): NodeOptions = {
    val dataPath = conf.get(BitlapConf.ROOT_DIR_LOCAL)
    val initConfStr = conf.get(BitlapConf.NODE_BIND_PEERS)
    val raftTimeout = conf.get(BitlapConf.NODE_RPC_TIMEOUT)
    FileUtils.forceMkdir(new File(dataPath))
    val logUri = StringEx.withPaths(dataPath, "raft", "log")
    val raftMetaUri = StringEx.withPaths(dataPath, "raft", "meta")
    val snapshotUri = StringEx.withPaths(dataPath, "raft", "snapshot")

    val nodeOptions = new NodeOptions()
    nodeOptions.setLogUri(logUri)
    nodeOptions.setRaftMetaUri(raftMetaUri)
    nodeOptions.setSnapshotUri(snapshotUri)
    nodeOptions.setFsm(new MetaStateMachine)
    nodeOptions.setElectionTimeoutMs(raftTimeout.toInt)
    nodeOptions.setDisableCli(false)

    val cc = new Configuration()
    cc.parse(initConfStr)
    nodeOptions.setInitialConf(cc)

    FileUtils.forceMkdir(new File(logUri))
    FileUtils.forceMkdir(new File(raftMetaUri))
    FileUtils.forceMkdir(new File(snapshotUri))
    nodeOptions
  }

  override def close(): Unit =
    this.synchronized {
      super.close()
      this.node.shutdown()
    }
}
