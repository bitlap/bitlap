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
import org.bitlap.common.LifeCycle
import org.bitlap.common.proto.driver.BCloseSession
import org.bitlap.common.proto.driver.BExecuteStatement
import org.bitlap.common.proto.driver.BFetchResults
import org.bitlap.common.proto.driver.BOpenSession
import org.bitlap.common.proto.rpc.HelloRpcPB
import org.bitlap.common.utils.withPaths
import org.bitlap.server.raft.cli.BCLIService
import org.bitlap.server.raft.cli.HelloRpcProcessor
import org.bitlap.server.raft.cli.SessionManager
import org.bitlap.server.raft.cli.rpc.CloseSessionProcessor
import org.bitlap.server.raft.cli.rpc.ExecuteStatementProcessor
import org.bitlap.server.raft.cli.rpc.FetchResultsProcessor
import org.bitlap.server.raft.cli.rpc.OpenSessionProcessor
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Desc: Endpoint of bitlap server
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/22
 */
open class BitlapServerEndpoint(private val conf: BitlapConf) : LifeCycle {

    private val started = AtomicBoolean(false)
    private lateinit var node: Node

    override fun start() {
        if (!this.started.compareAndSet(false, true)) {
            return
        }
        val groupId = "bitlap-cluster"
        val serverIdStr = conf.get(BitlapConf.NODE_BIND_HOST)
        val nodeOptions = extractOptions(conf)
        val serverId = PeerId().apply {
            require(parse(serverIdStr)) { "Fail to parse serverId:$serverIdStr" }
        }
        registerReq()
        registerResp()
        val rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.endpoint)
        registerProcessor(rpcServer)
        val raftGroupService = RaftGroupService(groupId, serverId, nodeOptions, rpcServer)
        this.node = raftGroupService.start()
        println("Started counter server at port:" + node.nodeId.peerId.port)
    }

    override fun isStarted(): Boolean = this.started.get()

    override fun close() {
        if (this.started.compareAndSet(true, false)) {
            this.node.shutdown()
        }
    }
}

private fun BitlapServerEndpoint.registerReq() {
    listOf(
        Pair(HelloRpcPB.Req::class.java.name, HelloRpcPB.Res.getDefaultInstance()),
        Pair(BOpenSession.BOpenSessionReq::class.java.name, BOpenSession.BOpenSessionResp.getDefaultInstance()),
        Pair(BCloseSession.BCloseSessionReq::class.java.name, BCloseSession.BCloseSessionResp.getDefaultInstance()),
        Pair(
            BExecuteStatement.BExecuteStatementReq::class.java.name,
            BExecuteStatement.BExecuteStatementReq.getDefaultInstance()
        ),
        Pair(BFetchResults.BFetchResultsReq::class.java.name, BFetchResults.BFetchResultsResp.getDefaultInstance()),
    ).forEach {
        RpcFactoryHelper.rpcFactory()
            .registerProtobufSerializer(it.first, it.second)
    }
}

private fun BitlapServerEndpoint.registerResp() {
    listOf(
        Pair(HelloRpcPB.Req::class.java.name, HelloRpcPB.Req.getDefaultInstance()),
        Pair(BOpenSession.BOpenSessionReq::class.java.name, BOpenSession.BOpenSessionReq.getDefaultInstance()),
        Pair(BCloseSession.BCloseSessionReq::class.java.name, BCloseSession.BCloseSessionReq.getDefaultInstance()),
        Pair(
            BExecuteStatement.BExecuteStatementReq::class.java.name,
            BExecuteStatement.BExecuteStatementReq.getDefaultInstance()
        ),
        Pair(BFetchResults.BFetchResultsReq::class.java.name, BFetchResults.BFetchResultsReq.getDefaultInstance()),
    ).forEach {
        MarshallerHelper.registerRespInstance(it.first, it.second)
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
