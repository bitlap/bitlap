package org.bitlap.network.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.network.core.CLIService
import org.bitlap.network.proto.driver.BGetColumns

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
