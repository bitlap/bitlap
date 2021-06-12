package org.bitlap.server.test

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.bitlap.server.BitlapServer

/**
 *
 * @author 梦境迷离
 * @since 2021/6/12
 * @version 1.0
 */
object Test {


    @JvmStatic
    fun main(args: Array<String>) {
        BitlapServer().start()

        val groupId = "bitlap-cluster"
        val conf = Configuration()
        conf.parse("localhost:23333")
        RouteTable.getInstance().updateConfiguration(groupId, conf)
        val cli = CliClientServiceImpl()
        cli.init(CliOptions())
        check(RouteTable.getInstance().refreshLeader(cli, groupId, 3000).isOk) { "Refresh leader failed" }
        val leader = RouteTable.getInstance().selectLeader(groupId)
        println("Leader is $leader")
        Thread.currentThread().join()
    }

}
