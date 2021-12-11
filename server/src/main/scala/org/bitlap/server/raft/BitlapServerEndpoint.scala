package org.bitlap.server.raft

import com.alipay.sofa.jraft.{ Node, RaftGroupService }
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.NodeOptions
import com.alipay.sofa.jraft.rpc.{ RaftRpcServerFactory, RpcServer }
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import org.apache.commons.io.FileUtils
import org.bitlap.common.{ BitlapConf, LifeCycleWrapper }
import org.bitlap.net.{ NetworkHelper, NetworkServiceImpl }
import org.bitlap.net.processor._
import org.bitlap.net.session.SessionManager

import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 *
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class BitlapServerEndpoint(private val conf: BitlapConf) extends LifeCycleWrapper with NetworkHelper {

  private var node: Node = _

  override def start() {
    this.synchronized {
      if (this.started) {
        return
      }
      super.start()
      val serverIdStr = conf.get(BitlapConf.getNODE_BIND_HOST.getGroup, BitlapConf.getNODE_BIND_HOST.getKey)
      val nodeOptions = extractOptions()
      val serverId = new PeerId()
      serverId.parse(serverIdStr)
      registerMessageInstances(NetworkHelper.requestInstances(), (t1, t2) =>
        RpcFactoryHelper.rpcFactory().registerProtobufSerializer(t1, t2)
      )
      registerMessageInstances(NetworkHelper.responseInstances(), { (t1, t2) =>
        MarshallerHelper.registerRespInstance(t1, t2)
      })
      val rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint)
      registerProcessor(rpcServer)
      val raftGroupService = new RaftGroupService(conf.get(BitlapConf.getNODE_GROUP_ID.getGroup, BitlapConf.getNODE_GROUP_ID.getKey), serverId, nodeOptions, rpcServer)
      this.node = raftGroupService.start()
      println("Started counter server at port:" + node.getNodeId.getPeerId.getPort)
    }

  }

  override def close() {
    this.synchronized {
      super.close()
      this.node.shutdown()
    }
  }

  def extractOptions(): NodeOptions = {
    val dataPath = conf.get(BitlapConf.getDEFAULT_ROOT_DIR_LOCAL.getGroup, BitlapConf.getDEFAULT_ROOT_DIR_LOCAL.getKey)
    val initConfStr = conf.get(BitlapConf.getNODE_BIND_PEERS.getGroup, BitlapConf.getNODE_BIND_PEERS.getKey)
    val timeout = conf.get(BitlapConf.getNODE_RAFT_TIMEOUT.getGroup, BitlapConf.getNODE_RAFT_TIMEOUT.getKey)
    val raftTimeout = if (timeout != null) timeout.toInt * 1000 else 1000
    FileUtils.forceMkdir(new File(dataPath))
    val logUri = "raft" + File.separator + "log"
    val raftMetaUri = "raft" + File.separator + "meta"
    val snapshotUri = "raft" + File.separator + "snapshot"

    val nodeOptions = new NodeOptions()
    nodeOptions.setLogUri(logUri)
    nodeOptions.setRaftMetaUri(raftMetaUri)
    nodeOptions.setSnapshotUri(snapshotUri)
    nodeOptions.setFsm(new MetaStateMachine)
    nodeOptions.setElectionTimeoutMs(raftTimeout)
    nodeOptions.setDisableCli(false)

    val cc = new Configuration()
    cc.parse(initConfStr)
    nodeOptions.setInitialConf(cc)

    FileUtils.forceMkdir(new File(logUri))
    FileUtils.forceMkdir(new File(raftMetaUri))
    FileUtils.forceMkdir(new File(snapshotUri))
    nodeOptions
  }

  private def registerProcessor(rpcServer: RpcServer) {
    val sessionManager = new SessionManager()
    Future {
      sessionManager.startListener()
    }
    val cliService = new NetworkServiceImpl(sessionManager)
    val processors = ProcessorsManager(cliService, null)
    List(
      processors.closeSession,
      processors.openSession,
      processors.executeStatement,
      processors.fetchResults,
      processors.getResultSet,
      processors.getSchemas,
      processors.getColumns,
      processors.getTables
    ).foreach {
      rpcServer.registerProcessor(_)
    }
  }
}

