package org.bitlap.server.raft

import com.alipay.sofa.jraft.Node
import com.alipay.sofa.jraft.RaftGroupService
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.NodeOptions
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory
import com.alipay.sofa.jraft.rpc.RpcServer
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import org.apache.commons.io.FileUtils
import org.bitlap.common.BitlapConf
import org.bitlap.common.LifeCycleWrapper
import org.bitlap.common.utils.withPaths
import org.bitlap.network.NetworkHelper
import org.bitlap.network.core.NetworkServiceImpl
import org.bitlap.network.core.SessionManager
import org.bitlap.network.processor.CloseSessionProcessor
import org.bitlap.network.processor.ExecuteStatementProcessor
import org.bitlap.network.processor.FetchResultsProcessor
import org.bitlap.network.processor.GetColumnsProcessor
import org.bitlap.network.processor.GetResultSetMetaDataProcessor
import org.bitlap.network.processor.GetSchemasProcessor
import org.bitlap.network.processor.GetTablesProcessor
import org.bitlap.network.processor.OpenSessionProcessor
import java.io.File

/**
 * Desc: Endpoint of bitlap server
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/22
 */
open class BitlapServerEndpoint(private val conf: BitlapConf) : LifeCycleWrapper(), NetworkHelper {

    private lateinit var node: Node

    @Synchronized
    override fun start() {
        if (this.started) {
            return
        }
        super.start()
        val serverIdStr = conf.get(BitlapConf.NODE_BIND_HOST)
        val nodeOptions = extractOptions(conf)
        val serverId = PeerId().apply {
            require(parse(serverIdStr)) { "Fail to parse serverId:$serverIdStr" }
        }
        registerMessageInstances(NetworkHelper.requestInstances()) {
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(it.first, it.second)
        }
        registerMessageInstances(NetworkHelper.responseInstances()) {
            MarshallerHelper.registerRespInstance(it.first, it.second)
        }
        val rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.endpoint)
        registerProcessor(rpcServer)
        val raftGroupService = RaftGroupService(conf.get(BitlapConf.NODE_GROUP_ID), serverId, nodeOptions, rpcServer)
        this.node = raftGroupService.start()
        println("Started counter server at port:" + node.nodeId.peerId.port)
    }

    @Synchronized
    override fun close() {
        super.close()
        this.node.shutdown()
    }
}

private fun registerProcessor(rpcServer: RpcServer) {
    val cliService = NetworkServiceImpl(SessionManager())
    listOf(
        CloseSessionProcessor(cliService),
        OpenSessionProcessor(cliService),
        ExecuteStatementProcessor(cliService),
        FetchResultsProcessor(cliService),
        GetResultSetMetaDataProcessor(cliService),
        GetSchemasProcessor(cliService),
        GetTablesProcessor(cliService),
        GetColumnsProcessor(cliService),
    ).forEach { rpcServer.registerProcessor(it) }
}

private fun extractOptions(conf: BitlapConf): NodeOptions {
    val dataPath = conf.get(BitlapConf.DEFAULT_ROOT_DIR_LOCAL)!!
    val initConfStr = conf.get(BitlapConf.NODE_BIND_PEERS)
    val raftTimeout: Int = conf.get(BitlapConf.NODE_RAFT_TIMEOUT).let { if (it.isNullOrEmpty()) 1 else it.toInt() } * 1000
    FileUtils.forceMkdir(File(dataPath))
    return NodeOptions().apply {
        logUri = dataPath.withPaths("raft", "log")
        raftMetaUri = dataPath.withPaths("raft", "meta")
        snapshotUri = dataPath.withPaths("raft", "snapshot")
        FileUtils.forceMkdir(File(logUri))
        FileUtils.forceMkdir(File(raftMetaUri))
        FileUtils.forceMkdir(File(snapshotUri))
        electionTimeoutMs = raftTimeout
        isDisableCli = false
        snapshotIntervalSecs = 30
        fsm = MetaStateMachine()
        initialConf = Configuration().apply {
            require(parse(initConfStr)) { "Fail to parse initConf: $initConfStr" }
        }
    }
}
