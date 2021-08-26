package org.bitlap.network.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.network.core.CLIService
import org.bitlap.network.proto.driver.BGetSchemas

/**
 * GetSchemas
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetSchemasProcessor(private val cliService: CLIService) :
    RpcProcessor<BGetSchemas.BGetSchemasReq>,
    BaseProcessor {
    override fun handleRequest(rpcCtx: RpcContext, request: BGetSchemas.BGetSchemasReq) {
    }

    override fun interest(): String = BGetSchemas.BGetSchemasReq::class.java.name
}
