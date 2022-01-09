package org.bitlap.server.raft

import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.NodeOptions
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.rpc.{RaftRpcServerFactory, RpcServer}
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import com.alipay.sofa.jraft.{Node, RaftGroupService}
import org.apache.commons.io.FileUtils
import org.bitlap.common.utils.StringEx
import org.bitlap.common.{BitlapConf, LifeCycleThread}
import org.bitlap.net.processor._
import org.bitlap.net.session.SessionManager
import org.bitlap.net.{NetworkHelper, NetworkServiceImpl}

import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 *
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class BitlapServerEndpoint(private val conf: BitlapConf) extends LifeCycleThread("BitlapServerEndpoint") with NetworkHelper {

  private var node: Node = _

  override def run(): Unit = {
    val serverIdStr = conf.get(BitlapConf.NODE_BIND_HOST)
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
    val raftGroupService = new RaftGroupService(conf.get(BitlapConf.NODE_GROUP_ID), serverId, nodeOptions, rpcServer)
    this.node = raftGroupService.start()
    println("Started counter server at port:" + node.getNodeId.getPeerId.getPort)
  }

  override def close(): Unit = {
    this.synchronized {
      super.close()
      this.node.shutdown()
    }
  }

  def extractOptions(): NodeOptions = {
    val dataPath = conf.get(BitlapConf.DEFAULT_ROOT_DIR_LOCAL)
    val initConfStr = conf.get(BitlapConf.NODE_BIND_PEERS)
    val raftTimeout = conf.get(BitlapConf.NODE_RAFT_TIMEOUT)
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

  private def registerProcessor(rpcServer: RpcServer): Unit = {
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

