package org.bitlap.server

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.bitlap.common.BitlapConf
import org.bitlap.common.proto.rpc.HelloRpcPB
import org.bitlap.server.raft.BitlapServerEndpoint

/**
 * Desc: Bitlap server
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/16
 */
class BitlapServer {

    private val conf = BitlapConf()
    private val server = BitlapServerEndpoint(conf)

    fun start() {
        this.server.start()
        Runtime.getRuntime().addShutdownHook(
            Thread {
                this.server.use { it.close() }
            }
        )
    }
}

fun main() {
    BitlapServer().start()

    val groupId = "bitlap-cluster"
    val conf = Configuration()
    conf.parse("localhost:8001")
    RouteTable.getInstance().updateConfiguration(groupId, conf)
    val cli = CliClientServiceImpl()
    cli.init(CliOptions())
    check(RouteTable.getInstance().refreshLeader(cli, groupId, 1000).isOk) { "Refresh leader failed" }
    val leader = RouteTable.getInstance().selectLeader(groupId)
    println("Leader is $leader")
    cli.rpcClient.invokeAsync(
        leader.endpoint, HelloRpcPB.Req.newBuilder().setPing("Bitlap Ping").build(),
        { result, err ->
            result as HelloRpcPB.Res
            println("==========================> ${result.pong}")
        },
        5000
    )
}
