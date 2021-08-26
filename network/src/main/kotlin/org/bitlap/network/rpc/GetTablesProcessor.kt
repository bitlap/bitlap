package org.bitlap.network.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.network.core.CLIService
import org.bitlap.network.proto.driver.BGetTables

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
