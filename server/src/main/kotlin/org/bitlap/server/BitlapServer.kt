package org.bitlap.server

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.bitlap.common.BitlapConf
import org.bitlap.server.raft.BitlapServerEndpoint

/**
 * Desc: Bitlap server
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/16
 */
class BitlapServer {

    val conf = BitlapConf()
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
    val server = BitlapServer()
    server.start()
    val groupId = server.conf.get(BitlapConf.NODE_GROUP_ID)
    val raftTimeout: Int = server.conf.get(BitlapConf.NODE_RAFT_TIMEOUT).let { if (it.isNullOrEmpty()) 1 else it.toInt() } * 1000

    val conf = Configuration()
    conf.parse("localhost:8001")
    RouteTable.getInstance().updateConfiguration(groupId, conf)
    val cli = CliClientServiceImpl()
    cli.init(CliOptions())
    check(RouteTable.getInstance().refreshLeader(cli, groupId, raftTimeout).isOk) { "Refresh leader failed" }
    val leader = RouteTable.getInstance().selectLeader(groupId)
    println("Leader is $leader")
}
