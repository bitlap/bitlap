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
import org.bitlap.common.RpcServiceSupport
import org.bitlap.common.utils.withPaths
import org.bitlap.server.raft.cli.BCLIService
import org.bitlap.server.raft.cli.HelloRpcProcessor
import org.bitlap.server.raft.cli.SessionManager
import org.bitlap.server.raft.cli.rpc.CloseSessionProcessor
import org.bitlap.server.raft.cli.rpc.ExecuteStatementProcessor
import org.bitlap.server.raft.cli.rpc.FetchResultsProcessor
import org.bitlap.server.raft.cli.rpc.OpenSessionProcessor
import java.io.File

/**
 * Desc: Endpoint of bitlap server
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/22
 */
open class BitlapServerEndpoint(private val conf: BitlapConf) : LifeCycleWrapper(), RpcServiceSupport {

    private lateinit var node: Node

    @Synchronized
    override fun start() {
        if (this.started) {
            return
        }
        super.start()
        val groupId = "bitlap-cluster"
        val serverIdStr = conf.get(BitlapConf.NODE_BIND_HOST)
        val nodeOptions = extractOptions(conf)
        val serverId = PeerId().apply {
            require(parse(serverIdStr)) { "Fail to parse serverId:$serverIdStr" }
        }
        registerMessageInstances(RpcServiceSupport.requestInstances()) {
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(it.first, it.second)
        }
        registerMessageInstances(RpcServiceSupport.responseInstances()) {
            MarshallerHelper.registerRespInstance(it.first, it.second)
        }
        val rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.endpoint)
        registerProcessor(rpcServer)
        val raftGroupService = RaftGroupService(groupId, serverId, nodeOptions, rpcServer)
        this.node = raftGroupService.start()
        println("Started counter server at port:" + node.nodeId.peerId.port)
    }

    @Synchronized
    override fun close() {
        super.close()
        this.node.shutdown()
    }
}

private fun BitlapServerEndpoint.registerProcessor(rpcServer: RpcServer) {
    val cliService = BCLIService(SessionManager())
    listOf(
        CloseSessionProcessor(cliService),
        OpenSessionProcessor(cliService),
        ExecuteStatementProcessor(cliService),
        FetchResultsProcessor(cliService),
        HelloRpcProcessor()
    ).forEach { rpcServer.registerProcessor(it) }
}

private fun BitlapServerEndpoint.extractOptions(conf: BitlapConf): NodeOptions {
    val dataPath = conf.get(BitlapConf.DEFAULT_ROOT_DIR_LOCAL)!!
    val initConfStr = conf.get(BitlapConf.NODE_BIND_PEERS)
    FileUtils.forceMkdir(File(dataPath))
    return NodeOptions().apply {
        logUri = dataPath.withPaths("raft", "log")
        raftMetaUri = dataPath.withPaths("raft", "meta")
        snapshotUri = dataPath.withPaths("raft", "snapshot")
        electionTimeoutMs = 1000
        isDisableCli = false
        snapshotIntervalSecs = 30
        fsm = MetaStateMachine()
        initialConf = Configuration().apply {
            require(parse(initConfStr)) { "Fail to parse initConf: $initConfStr" }
        }
    }
}
