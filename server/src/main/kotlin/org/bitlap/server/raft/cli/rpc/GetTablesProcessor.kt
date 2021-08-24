package org.bitlap.server.raft.cli.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.driver.BGetTables
import org.bitlap.server.raft.cli.CLIService

/**
 * GetTables
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetTablesProcessor(private val cliService: CLIService) :
    RpcProcessor<BGetTables.BGetTablesReq>,
    BaseProcessor {
    override fun handleRequest(rpcCtx: RpcContext, request: BGetTables.BGetTablesReq) {
    }

    override fun interest(): String = BGetTables.BGetTablesReq::class.java.name
}
