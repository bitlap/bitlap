package org.bitlap.server.raft.cli.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.driver.BGetColumns
import org.bitlap.server.raft.cli.CLIService

/**
 * GetColumns
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetColumnsProcessor(private val cliService: CLIService) :
    RpcProcessor<BGetColumns.BGetColumnsReq>,
    BaseProcessor {
    override fun handleRequest(rpcCtx: RpcContext, request: BGetColumns.BGetColumnsReq) {
    }

    override fun interest(): String = BGetColumns.BGetColumnsReq::class.java.name
}
