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
import org.bitlap.common.BitlapConf
import org.bitlap.common.LifeCycle
import org.bitlap.common.proto.rpc.HelloRpcPB
import org.bitlap.common.utils.withPaths
import org.bitlap.server.raft.rpc.HelloRpcProcessor
import java.io.File

/**
 * Desc: Endpoint of bitlap server
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/22
 */
open class BitlapServerEndpoint(val conf: BitlapConf) : LifeCycle {

    @Volatile
    private var started = false

    @Volatile
    private var shutdown = true

    private lateinit var node: Node

    override fun start() {
        if (this.started) {
            return
        }
        val groupId = "bitlap-cluster"
        val dataPath = conf.get(BitlapConf.DEFAULT_ROOT_DIR_LOCAL)!!
        val serverIdStr = conf.get(BitlapConf.NODE_BIND_HOST)
        val initConfStr = conf.get(BitlapConf.NODE_BIND_PEERS)

        FileUtils.forceMkdir(File(dataPath))
        val nodeOptions = NodeOptions().apply {
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

        val serverId = PeerId().apply {
            require(parse(serverIdStr)) { "Fail to parse serverId:$serverIdStr" }
        }

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
