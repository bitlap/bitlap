package org.bitlap.server.raft

import com.alipay.sofa.jraft.Node
import com.alipay.sofa.jraft.RaftGroupService
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.NodeOptions
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import org.apache.commons.io.FileUtils
import org.bitlap.common.LifeCycle
import org.bitlap.common.proto.rpc.HelloRpcPB
import org.bitlap.server.raft.rpc.HelloRpcProcessor
import java.io.File

/**
 * Desc: Endpoint of bitlap server
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/22
 */
open class BitlapServerEndpoint : LifeCycle {

    @Volatile
    private var started = false

    @Volatile
    private var shutdown = true

    private lateinit var node: Node

    override fun start() {
        if (this.started) {
            return
        }
        val dataPath = "/usr/local/var/bitlap"
        val groupId = "bitlap-cluster"
        val serverIdStr = "localhost:8001"
        val initConfStr = "localhost:8001"

        FileUtils.forceMkdir(File(dataPath))

        val nodeOptions = NodeOptions()
        nodeOptions.electionTimeoutMs = 1000
        nodeOptions.isDisableCli = false
        nodeOptions.snapshotIntervalSecs = 30

        val serverId = PeerId()
        require(serverId.parse(serverIdStr)) { "Fail to parse serverId:$serverIdStr" }
        val initConf = Configuration()
        require(initConf.parse(initConfStr)) { "Fail to parse initConf:$initConfStr" }
        nodeOptions.initialConf = initConf
        nodeOptions.fsm = MetaStateMachine()
        nodeOptions.logUri = dataPath + File.separator + "log"
        nodeOptions.raftMetaUri = dataPath + File.separator + "raft_meta"
        nodeOptions.snapshotUri = dataPath + File.separator + "snapshot"

        RpcFactoryHelper.rpcFactory().registerProtobufSerializer(HelloRpcPB.Req::class.java.name, HelloRpcPB.Req.getDefaultInstance())
        MarshallerHelper.registerRespInstance(HelloRpcPB.Req::class.java.name, HelloRpcPB.Res.getDefaultInstance())
        val rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.endpoint)
        rpcServer.registerProcessor(HelloRpcProcessor())

        val raftGroupService = RaftGroupService(groupId, serverId, nodeOptions, rpcServer)
        this.node = raftGroupService.start()

        this.started = true
        this.shutdown = false

        println("Started counter server at port:" + node.nodeId.peerId.port)
    }

    override fun isStarted(): Boolean = this.started
    override fun isShutdown(): Boolean = this.shutdown

    override fun close() {
        this.node.shutdown()
    }
}
